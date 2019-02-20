package soak_clients

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"strings"

	"github.com/confluentinc/ce-kafka/cc-services/trogdor"
	"github.com/dariubs/percent"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"io/ioutil"
	"math/rand"
	"time"
)

type SoakTestConfig struct {
	Topics                   []TopicConfiguration `json:"topics"`
	LongLivedTaskDurationMs  uint64               `json:"long_lived_task_duration_ms"`
	ShortLivedTaskDurationMs uint64               `json:"short_lived_task_duration_ms"`
}

func (t *SoakTestConfig) parseConfig(configPath string) error {
	raw, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed reading the soak test configuration from %s", configPath))
	}

	err = json.Unmarshal(raw, &t)
	if err != nil {
		return err
	}

	topicNames := make([]string, len(t.Topics))
	for _, topicConfig := range t.Topics {
		topicNames = append(topicNames, topicConfig.Name)
	}
	logutil.Info(logger, fmt.Sprintf("Loaded configuration for topics %s", strings.Join(topicNames, ",")))
	return nil
}

type TopicConfiguration struct {
	Name                 string  `json:"name"`
	PartitionsCount      int     `json:"partitions_count"`
	ProduceMBsThroughput float32 `json:"produce_mbs_throughput"`
	ConsumeMBsThroughput float32 `json:"consume_mbs_throughput"`
	ProduceCount         int     `json:"producer_count"`
	ConsumeCount         int     `json:"consumer_count"`
	TransactionsEnabled  bool    `json:"transactions_enabled"`
	IdempotenceEnabled   bool    `json:"idempotence_enabled"`
}

var baseProducerOptions = trogdor.ProducerOptions{
	ValueGenerator: trogdor.DefaultValueGeneratorSpec, // 1000 bytes size
	KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
}
var transactionalProducerOptions = trogdor.ProducerOptions{
	ValueGenerator:       trogdor.DefaultValueGeneratorSpec, // 1000 bytes size
	KeyGenerator:         trogdor.DefaultKeyGeneratorSpec,
	TransactionGenerator: trogdor.DefaultTransactionGeneratorSpec,
}

// Returns all the baseline tasks that should be ran on the Soak Cluster at all times
func baselineTasks(soakConfigPath string, trogdorAgentsCount int) ([]trogdor.TaskSpec, error) {
	var tasks []trogdor.TaskSpec
	var clientNodes []string
	for agentID := 0; agentID < trogdorAgentsCount; agentID++ {
		// should be the same as the one in agentStatefulSet.yaml
		clientNodes = append(clientNodes, fmt.Sprintf("cc-trogdor-service-agent-%d", agentID))
	}

	configuration := SoakTestConfig{}
	err := configuration.parseConfig(soakConfigPath)
	if err != nil {
		return []trogdor.TaskSpec{}, err
	}
	existingIDs := make(map[string]bool)
	for _, topicConfig := range configuration.Topics {
		newTasks := createTopicTasks(topicConfig, configuration.LongLivedTaskDurationMs, configuration.ShortLivedTaskDurationMs, clientNodes)
		// validate against duplicate IDs
		for _, newTask := range newTasks {
			if existingIDs[newTask.ID] {
				return tasks, errors.New(fmt.Sprintf("Duplicate task with ID %s", newTask.ID))
			}
			existingIDs[newTask.ID] = true
		}
		tasks = append(tasks, newTasks...)
	}

	return tasks, nil
}

// Creates Trogdor Produce and Consume Bench Tasks from a TopicConfiguration
// 50% of the clients are made long-lived whereas
// the rest are short-lived, scheduled to run until the long-lived tasks finish
func createTopicTasks(topicConfig TopicConfiguration, longLivedMs uint64, shortLivedMs uint64, clientNodes []string) []trogdor.TaskSpec {
	var tasks []trogdor.TaskSpec
	topic := trogdor.TopicSpec{
		NumPartitions:     uint64(topicConfig.PartitionsCount),
		ReplicationFactor: 3,
		TopicName:         topicConfig.Name,
	}
	clientCounts := calculateClientCounts(topicConfig)
	logutil.Debug(logger, "clientCounts: %+v", clientCounts)
	var producerOptions trogdor.ProducerOptions
	if topicConfig.TransactionsEnabled {
		producerOptions = transactionalProducerOptions
	} else {
		producerOptions = baseProducerOptions
	}

	var producerAdminConfig = trogdor.AdminConf{}
	copier.Copy(&producerAdminConfig, &adminConfig)
	if topicConfig.IdempotenceEnabled {
		producerAdminConfig.EnableIdempotence = "true"
	}
	consumerOptions := trogdor.ConsumerOptions{
		ConsumerGroup: fmt.Sprintf("Consume%sTestGroup", topicConfig.Name),
	}

	longLivingProducersScenarioConfig := scenarioConfig(&SoakScenarioConfig{
		scenarioId:         fmt.Sprintf("LongLivedProduce-%s", topicConfig.Name),
		class:              trogdor.PRODUCE_BENCH_SPEC_CLASS,
		durationMs:         longLivedMs,
		agentCount:         clientCounts.LongLivedProducersCount,
		topic:              topic,
		throughputMbPerSec: topicConfig.ProduceMBsThroughput * calculatePercentage(clientCounts.LongLivedProducersCount, topicConfig.ProduceCount),
		producerOptions:    producerOptions,
		adminConfig:        producerAdminConfig,
	}, shuffleSlice(clientNodes)) // shuffle the clientNodes order to ensure random distribution of tasks
	logutil.Debug(logger, "longLivingProducersScenarioConfig: %+v", longLivingProducersScenarioConfig)
	longLivingProducersScenario := &trogdor.ScenarioSpec{}
	longLivingProducersScenario.CreateScenario(longLivingProducersScenarioConfig)
	longLivingConsumersScenarioConfig := scenarioConfig(&SoakScenarioConfig{
		scenarioId:         fmt.Sprintf("LongLivedConsume-%s", topicConfig.Name),
		class:              trogdor.CONSUME_BENCH_SPEC_CLASS,
		durationMs:         longLivedMs,
		agentCount:         clientCounts.LongLivedConsumersCount,
		topic:              topic,
		throughputMbPerSec: topicConfig.ConsumeMBsThroughput * calculatePercentage(clientCounts.LongLivedConsumersCount, topicConfig.ConsumeCount),
		consumerOptions:    consumerOptions,
		adminConfig:        adminConfig,
	}, shuffleSlice(clientNodes))
	logutil.Debug(logger, "longLivingConsumersScenarioConfig: %+v", longLivingConsumersScenarioConfig)
	longLivingConsumersScenario := &trogdor.ScenarioSpec{}
	longLivingConsumersScenario.CreateScenario(longLivingConsumersScenarioConfig)
	tasks = append(tasks, longLivingProducersScenario.TaskSpecs...)
	tasks = append(tasks, longLivingConsumersScenario.TaskSpecs...)

	// schedule short-lived produce/consume tasks one week in advance
	nowMs := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	shortLivedProducersSoakConfig := &SoakScenarioConfig{
		scenarioId:         fmt.Sprintf("ShortLivedProduce-%s", topicConfig.Name),
		class:              trogdor.PRODUCE_BENCH_SPEC_CLASS,
		durationMs:         shortLivedMs,
		agentCount:         clientCounts.ShortLivedProducersCount,
		topic:              topic,
		throughputMbPerSec: topicConfig.ProduceMBsThroughput * calculatePercentage(clientCounts.ShortLivedProducersCount, topicConfig.ProduceCount),
		producerOptions:    producerOptions,
		startMs:            nowMs,
		adminConfig:        producerAdminConfig,
	}
	logutil.Debug(logger, "initial shortLivedProducersSoakConfig: %+v", shortLivedProducersSoakConfig)
	producerTasks, err := consecutiveTasks(shortLivedProducersSoakConfig, nowMs+longLivedMs, shuffleSlice(clientNodes))
	if err != nil {
		panic(err)
	}
	for _, config := range producerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}
	shortLivedConsumersSoakConfig := &SoakScenarioConfig{
		scenarioId:         fmt.Sprintf("ShortLivedConsume-%s", topicConfig.Name),
		class:              trogdor.CONSUME_BENCH_SPEC_CLASS,
		durationMs:         shortLivedMs,
		agentCount:         clientCounts.ShortLivedConsumersCount,
		topic:              topic,
		throughputMbPerSec: topicConfig.ConsumeMBsThroughput * calculatePercentage(clientCounts.ShortLivedConsumersCount, topicConfig.ConsumeCount),
		consumerOptions:    consumerOptions,
		startMs:            nowMs,
		adminConfig:        adminConfig,
	}
	logutil.Debug(logger, "initial shortLivedConsumersSoakConfig: %+v", shortLivedConsumersSoakConfig)
	consumerTasks, err := consecutiveTasks(shortLivedConsumersSoakConfig, nowMs+longLivedMs, shuffleSlice(clientNodes))
	if err != nil {
		panic(err)
	}
	for _, config := range consumerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}

	return tasks
}

func consecutiveTasks(soakConfig *SoakScenarioConfig, endMs uint64, clientNodes []string) ([]trogdor.ScenarioConfig, error) {
	var configs []trogdor.ScenarioConfig
	if soakConfig.startMs == 0 {
		return configs, errors.New("StartMs cannot be 0")
	}
	originalId := soakConfig.scenarioId
	for {
		if soakConfig.startMs >= endMs {
			break
		}
		configs = append(configs, scenarioConfig(soakConfig, clientNodes))
		newStartMs := soakConfig.startMs + soakConfig.durationMs
		soakConfig.startMs = newStartMs
		soakConfig.scenarioId = fmt.Sprintf("%s-%d", originalId, newStartMs)
	}
	return configs, nil
}

type SoakScenarioConfig struct {
	scenarioId         string
	class              string
	durationMs         uint64
	agentCount         int
	startMs            uint64
	topic              trogdor.TopicSpec
	throughputMbPerSec float32
	adminConfig        trogdor.AdminConf
	producerOptions    trogdor.ProducerOptions
	consumerOptions    trogdor.ConsumerOptions
}

func scenarioConfig(config *SoakScenarioConfig, clientNodes []string) trogdor.ScenarioConfig {
	// message sizes are approximately 1000 bytes using the default value generator
	messageSizeBytes := 1000
	throughputBytesPerSec := float64(config.throughputMbPerSec * 1000000)
	messagesStepPerLoop := uint64(throughputBytesPerSec / float64(messageSizeBytes))

	return trogdor.ScenarioConfig{
		ScenarioID:              config.scenarioId,
		Class:                   config.class,
		AgentCount:              config.agentCount,
		TopicSpec:               config.topic,
		LoopDurationMs:          config.durationMs,
		NumberOfLoops:           0,
		StartMs:                 config.startMs, // start immediately if not specified
		LoopCoolDownMs:          0,
		BootstrapServers:        bootstrapServers,
		MinimumMessagesPerAgent: 0,
		MessagesStepPerLoop:     messagesStepPerLoop,
		AdminConf:               config.adminConfig,
		ProducerOptions:         config.producerOptions,
		ConsumerOptions:         config.consumerOptions,
		ClientNodes:             clientNodes,
	}
}

type ClientCounts struct {
	LongLivedProducersCount  int
	LongLivedConsumersCount  int
	ShortLivedProducersCount int
	ShortLivedConsumersCount int
}

// Splits the total clients count by 50% long-lived and 50% short-lived.
// If the number cannot be evenly divided, more clients are allocated as long-lived
func calculateClientCounts(topicConfig TopicConfiguration) ClientCounts {
	return ClientCounts{
		LongLivedProducersCount:  topicConfig.ProduceCount - (topicConfig.ProduceCount / 2),
		ShortLivedProducersCount: topicConfig.ProduceCount / 2,
		LongLivedConsumersCount:  topicConfig.ConsumeCount - (topicConfig.ConsumeCount / 2),
		ShortLivedConsumersCount: topicConfig.ConsumeCount / 2,
	}
}

func calculatePercentage(part int, all int) float32 {
	return float32(percent.PercentOf(part, all)) / 100
}

// Fisher-Yates shuffle
func shuffleSlice(vals []string) []string {
	n := len(vals)
	cpy := make([]string, n)
	copy(cpy, vals)
	rand.Seed(time.Now().UnixNano())
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		cpy[i], cpy[j] = cpy[j], cpy[i]
	}
	return cpy
}
