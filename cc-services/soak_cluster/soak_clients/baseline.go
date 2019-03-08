package soak_clients

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"strings"

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
	for i, topicConfig := range t.Topics {
		topicNames[i] = topicConfig.Name
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
		newTasks := createTopicTasks(topicConfig, configuration.LongLivedTaskDurationMs,
			configuration.ShortLivedTaskDurationMs, clientNodes, existingIDs)
		tasks = append(tasks, newTasks...)
	}

	return tasks, nil
}

// Creates Trogdor Produce and Consume Bench Tasks from a TopicConfiguration
// 50% of the clients are made long-lived whereas
// the rest are short-lived, scheduled to run until the long-lived tasks finish
func createTopicTasks(topicConfig TopicConfiguration, longLivedMs uint64, shortLivedMs uint64,
	clientNodes []string, existingTaskIDs map[string]bool) []trogdor.TaskSpec {
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
	nowMs := uint64(time.Now().UnixNano() / int64(time.Millisecond))

	longLivingProducersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "LongLivedProduce",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.PRODUCE_BENCH_SPEC_CLASS,
		TaskCount:        clientCounts.LongLivedProducersCount,
		TopicSpec:        topic,
		DurationMs:       longLivedMs,
		StartMs:          0, // start immediately
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.ProduceMBsThroughput*calculatePercentage(clientCounts.LongLivedProducersCount, topicConfig.ProduceCount), producerOptions),
		AdminConf:        producerAdminConfig,
		ProducerOptions:  producerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}
	logutil.Debug(logger, "longLivingProducersScenarioConfig: %+v", longLivingProducersScenarioConfig)
	longLivingProducersScenario := &trogdor.ScenarioSpec{
		UsedNames: existingTaskIDs,
	}
	longLivingProducersScenario.CreateScenario(longLivingProducersScenarioConfig)

	longLivingConsumersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "LongLivedConsume",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		TaskCount:        clientCounts.LongLivedConsumersCount,
		TopicSpec:        topic,
		DurationMs:       longLivedMs,
		StartMs:          0, // start immediately
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.ConsumeMBsThroughput*calculatePercentage(clientCounts.LongLivedConsumersCount, topicConfig.ConsumeCount), producerOptions),
		AdminConf:        adminConfig,
		ConsumerOptions:  consumerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "longLivingConsumersScenarioConfig: %+v", longLivingConsumersScenarioConfig)
	longLivingConsumersScenario := &trogdor.ScenarioSpec{
		UsedNames: existingTaskIDs,
	}
	longLivingConsumersScenario.CreateScenario(longLivingConsumersScenarioConfig)
	tasks = append(tasks, longLivingProducersScenario.TaskSpecs...)
	tasks = append(tasks, longLivingConsumersScenario.TaskSpecs...)

	// schedule short-lived produce/consume tasks for one week in advance
	shortLivedProducersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedProduce",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.PRODUCE_BENCH_SPEC_CLASS,
		DurationMs:       shortLivedMs,
		TaskCount:        clientCounts.ShortLivedProducersCount,
		TopicSpec:        topic,
		StartMs:          nowMs,
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.ProduceMBsThroughput*calculatePercentage(clientCounts.ShortLivedProducersCount, topicConfig.ProduceCount), producerOptions),
		AdminConf:        producerAdminConfig,
		ProducerOptions:  producerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "initial shortLivedProducersScenarioConfig: %+v", shortLivedProducersScenarioConfig)
	producerTasks, err := consecutiveTasks(shortLivedProducersScenarioConfig, nowMs+longLivedMs)
	if err != nil {
		panic(err)
	}
	for _, config := range producerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{
			UsedNames: existingTaskIDs,
		}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}

	shortLivedConsumersScenarioConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     topic.TopicName,
			StartMs:  nowMs,
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		DurationMs:       shortLivedMs,
		TaskCount:        clientCounts.ShortLivedConsumersCount,
		TopicSpec:        topic,
		StartMs:          nowMs,
		BootstrapServers: bootstrapServers,
		MessagesPerSec:   messagesPerSec(topicConfig.ConsumeMBsThroughput*calculatePercentage(clientCounts.ShortLivedConsumersCount, topicConfig.ConsumeCount), producerOptions),
		AdminConf:        adminConfig,
		ConsumerOptions:  consumerOptions,
		ClientNodes:      shuffleSlice(clientNodes),
	}

	logutil.Debug(logger, "initial shortLivedConsumersScenarioConfig: %+v", shortLivedConsumersScenarioConfig)
	consumerTasks, err := consecutiveTasks(shortLivedConsumersScenarioConfig, nowMs+longLivedMs)
	if err != nil {
		panic(err)
	}
	for _, config := range consumerTasks {
		shortLivedProducersScenario := &trogdor.ScenarioSpec{
			UsedNames: existingTaskIDs,
		}
		shortLivedProducersScenario.CreateScenario(config)
		tasks = append(tasks, shortLivedProducersScenario.TaskSpecs...)
	}

	return tasks
}

func consecutiveTasks(initialScenario trogdor.ScenarioConfig, endMs uint64) ([]trogdor.ScenarioConfig, error) {
	var configs []trogdor.ScenarioConfig
	if initialScenario.StartMs == 0 {
		return configs, errors.New("StartMs cannot be 0")
	}
	originalId := initialScenario.ScenarioID
	durationMs := initialScenario.DurationMs
	nextScenario := trogdor.ScenarioConfig{
		StartMs: initialScenario.StartMs,
	}
	copier.Copy(&nextScenario, &initialScenario)

	for {
		if nextScenario.StartMs >= endMs {
			break
		}
		configs = append(configs, nextScenario)

		taskId := trogdor.TaskId{}
		newStartMs := nextScenario.StartMs + durationMs

		copier.Copy(&taskId, &originalId)
		copier.Copy(&nextScenario, &initialScenario)

		taskId.StartMs = newStartMs
		nextScenario.StartMs = newStartMs
		nextScenario.ScenarioID = taskId
	}
	return configs, nil
}

// Returns the number of messages per second we would need in order to achieve the desired throughput in MBs
func messagesPerSec(throughputMbPerSec float32, producerOptions trogdor.ProducerOptions) uint64 {
	messageSizeBytes := producerOptions.KeyGenerator.Size + producerOptions.ValueGenerator.Size
	throughputBytesPerSec := float64(throughputMbPerSec * 1000000)
	return uint64(throughputBytesPerSec / float64(messageSizeBytes))
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
