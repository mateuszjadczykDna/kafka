package soak_clients

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

const (
	oneWeekDurationMs        = 604800000
	fifteenMinutesDurationMs = 900000
)

var clientNodes = []string{"node1", "node2", "node3"}

func TestBaselineTasks(t *testing.T) {
	InitLogger()
	topics := soakTestConfig([]string{"a", "b", "c"})
	produceCount, consumeCount := 0, 0
	for _, topic := range topics.Topics {
		consumeCount += topic.ConsumeCount
		produceCount += topic.ProduceCount
	}
	expected := calculateExpectedTasksCount(produceCount, consumeCount, topics.LongLivedTaskDurationMs, topics.ShortLivedTaskDurationMs)
	configPath := writeSoakTestConfigFile(t, topics)
	tasks, err := baselineTasks(configPath, 10)
	if err != nil {
		fmt.Println(err)
		assert.Fail(t, "error while getting baseline tasks")
	}

	assertTaskNamesUnique(t, tasks)
	assertTaskCount(t, tasks, expected, topics.LongLivedTaskDurationMs, topics.ShortLivedTaskDurationMs)
}

func TestCreateTopicTasks(t *testing.T) {
	produceCount := 12
	consumeCount := 10

	expected := calculateExpectedTasksCount(produceCount, consumeCount, oneWeekDurationMs, fifteenMinutesDurationMs/2)
	topicSpecification := TopicConfiguration{
		Name:                 "testTest",
		PartitionsCount:      1000,
		ProduceMBsThroughput: 60,
		ConsumeMBsThroughput: 60,
		ProduceCount:         produceCount,
		ConsumeCount:         consumeCount,
	}
	tasks := createTopicTasks(topicSpecification, oneWeekDurationMs, fifteenMinutesDurationMs/2, clientNodes, make(map[string]bool))

	assertTaskCount(t, tasks, expected, oneWeekDurationMs, fifteenMinutesDurationMs/2)
}

func assertTaskNamesUnique(t *testing.T, tasks []trogdor.TaskSpec) {
	existingIDs := make(map[string]bool)
	for _, newTask := range tasks {
		if existingIDs[newTask.ID] {
			assert.Fail(t, fmt.Sprintf("Task name %s is duplicate", newTask.ID))
			return
		}
		existingIDs[newTask.ID] = true
	}
}

func assertTaskCount(t *testing.T, tasks []trogdor.TaskSpec, expected expectedTasksCount,
	longLivedMs uint64, shortLivedMs uint64) {
	isShortLivedTask := func(v string) bool {
		return strings.Contains(v, fmt.Sprintf(`"durationMs":%d,`, shortLivedMs))
	}
	isLongLivedTask := func(v string) bool {
		return strings.Contains(v, fmt.Sprintf(`"durationMs":%d,`, longLivedMs))
	}
	isConsumerTask := func(v string) bool {
		return strings.Contains(v, trogdor.CONSUME_BENCH_SPEC_CLASS)
	}
	isProducerTask := func(v string) bool {
		return strings.Contains(v, trogdor.PRODUCE_BENCH_SPEC_CLASS)
	}
	taskJsons := make([]string, len(tasks))
	for i, v := range tasks {
		buffer, err := v.Spec.MarshalJSON()
		if err != nil {
			assert.Fail(t, "received error while unmarshalling JSON", err)
		}
		taskJsons[i] = string(buffer)
	}

	shortLivedTasks := filter(taskJsons, isShortLivedTask)
	longLivedTasks := filter(taskJsons, isLongLivedTask)
	assert.Equal(t, expected.ExpectedShortLivedTasksCount, len(shortLivedTasks))
	assert.Equal(t, expected.ExpectedLongLivedTasksCount, len(longLivedTasks))

	shortLivedConsumerTasks := filter(taskJsons, func(v string) bool {
		return isShortLivedTask(v) && isConsumerTask(v)
	})
	shortLivedProducerTasks := filter(taskJsons, func(v string) bool {
		return isShortLivedTask(v) && isProducerTask(v)
	})
	longLivedConsumerTasks := filter(taskJsons, func(v string) bool {
		return isLongLivedTask(v) && isConsumerTask(v)
	})
	longLivedProducerTasks := filter(taskJsons, func(v string) bool {
		return isLongLivedTask(v) && isProducerTask(v)
	})
	assert.Equal(t, expected.ExpectedShortLivedConsumerTasks, len(shortLivedConsumerTasks))
	assert.Equal(t, expected.ExpectedShortLivedProducerTasks, len(shortLivedProducerTasks))
	assert.Equal(t, expected.ExpectedLongLivedProducerTasks, len(longLivedProducerTasks))
	assert.Equal(t, expected.ExpectedLongLivedConsumerTasks, len(longLivedConsumerTasks))
}

func TestParseConfigParsesCorrectly(t *testing.T) {
	originalTopics := soakTestConfig([]string{"a", "b", "c"})
	fileName := writeSoakTestConfigFile(t, originalTopics)

	topics := SoakTestConfig{}
	err := topics.parseConfig(fileName)

	assert.NoError(t, err)
	assert.Equal(t, originalTopics, topics)
}

func writeSoakTestConfigFile(t *testing.T, topics SoakTestConfig) string {
	file, err := ioutil.TempFile("/tmp", "config")
	data, err := json.Marshal(topics)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while marshalling topics %s", err))
	}
	file.Write(data)

	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while creating temporary file %s", err))
	}

	return file.Name()
}

func TestConsecutiveTasks(t *testing.T) {
	mediumTopic := trogdor.TopicSpec{
		NumPartitions:     16,
		ReplicationFactor: 3,
	}
	startConfig := trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     "MediumTopic",
		},
		Class:            trogdor.CONSUME_BENCH_SPEC_CLASS,
		DurationMs:       5,
		TaskCount:        3,
		TopicSpec:        mediumTopic,
		BootstrapServers: bootstrapServers,
		StartMs:          10,
		MessagesPerSec:   750,
		AdminConf:        adminConfig,
		ConsumerOptions: trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		ClientNodes: shuffleSlice(clientNodes),
	}
	firstConfig := trogdor.ScenarioConfig{}
	copier.Copy(&firstConfig, &startConfig)
	secondConfig := trogdor.ScenarioConfig{}
	copier.Copy(&secondConfig, &startConfig)
	secondConfig.StartMs = 15
	secondConfig.ScenarioID = trogdor.TaskId{
		TaskType: "ShortLivedConsume",
		Desc:     "MediumTopic",
		StartMs:  15,
	}
	thirdConfig := trogdor.ScenarioConfig{}
	copier.Copy(&thirdConfig, &startConfig)
	thirdConfig.StartMs = 20
	thirdConfig.ScenarioID = trogdor.TaskId{
		TaskType: "ShortLivedConsume",
		Desc:     "MediumTopic",
		StartMs:  20,
	}

	// should return 3 scenario configs
	expectedConfigs := []trogdor.ScenarioConfig{
		firstConfig,
		secondConfig,
		thirdConfig,
	}

	actualConfigs, err := consecutiveTasks(startConfig, 25)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	assert.Equal(t, expectedConfigs, actualConfigs)
}

func TestConsecutiveTasksFailsIfStartMsIsZero(t *testing.T) {
	mediumTopic := trogdor.TopicSpec{
		NumPartitions:     16,
		ReplicationFactor: 3,
	}
	startConfig := &trogdor.ScenarioConfig{
		ScenarioID: trogdor.TaskId{
			TaskType: "ShortLivedConsume",
			Desc:     "MediumTopic",
		},
		Class:          trogdor.CONSUME_BENCH_SPEC_CLASS,
		TaskCount:      3,
		TopicSpec:      mediumTopic,
		MessagesPerSec: 75,
		ConsumerOptions: trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		DurationMs:       5,
		BootstrapServers: bootstrapServers,
	}
	_, err := consecutiveTasks(*startConfig, 25)
	assert.Error(t, err)
}

func TestMessagesPerSec(t *testing.T) {
	keyGen, valueGen := trogdor.KeyGeneratorSpec{Size: 100}, trogdor.ValueGeneratorSpec{Size: 900}
	expectedMessagesPerSec := uint64(1000) // 1000 messages of size 0.01mb per second equal 1mb/s throughput

	assert.Equal(t,
		expectedMessagesPerSec,
		messagesPerSec(1.00, trogdor.ProducerOptions{
			ValueGenerator:       valueGen,
			TransactionGenerator: trogdor.DefaultTransactionGeneratorSpec,
			KeyGenerator:         keyGen,
		}))
}

func TestCalculateClientCounts(t *testing.T) {
	expectedCounts := ClientCounts{
		LongLivedProducersCount:  6,
		LongLivedConsumersCount:  5,
		ShortLivedProducersCount: 5,
		ShortLivedConsumersCount: 5,
	}
	clientCounts := calculateClientCounts(TopicConfiguration{
		Name:                 "",
		PartitionsCount:      1000,
		ProduceMBsThroughput: 60,
		ConsumeMBsThroughput: 60,
		ProduceCount:         11,
		ConsumeCount:         10,
	})

	assert.Equal(t, expectedCounts, clientCounts)
}

func soakTestConfig(topicNames []string) SoakTestConfig {
	topics := SoakTestConfig{
		LongLivedTaskDurationMs:  oneWeekDurationMs,
		ShortLivedTaskDurationMs: fifteenMinutesDurationMs,
	}
	for _, topicName := range topicNames {
		topics.Topics = append(topics.Topics, TopicConfiguration{
			Name:                 topicName,
			PartitionsCount:      1000,
			ProduceMBsThroughput: 60,
			ConsumeMBsThroughput: 60,
			ProduceCount:         10,
			ConsumeCount:         10,
		})
	}

	return topics
}

func filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

type expectedTasksCount struct {
	ExpectedShortLivedProducerTasks int
	ExpectedLongLivedProducerTasks  int
	ExpectedShortLivedConsumerTasks int
	ExpectedLongLivedConsumerTasks  int
	ExpectedShortLivedTasksCount    int
	ExpectedLongLivedTasksCount     int
	ExpectedTotalTasksCount         int
}

// calculate the total expected task counts for a given topic, given the number of producer and consumer clients
func calculateExpectedTasksCount(produceCount int, consumeCount int, longLivedMs uint64, shortLivedMs uint64) expectedTasksCount {
	clientCounts := calculateClientCounts(TopicConfiguration{
		ProduceCount: produceCount,
		ConsumeCount: consumeCount,
	})
	shortLivedCount := int(longLivedMs / shortLivedMs)
	expected := expectedTasksCount{
		ExpectedLongLivedProducerTasks:  clientCounts.LongLivedProducersCount,
		ExpectedLongLivedConsumerTasks:  clientCounts.LongLivedConsumersCount,
		ExpectedShortLivedProducerTasks: shortLivedCount * clientCounts.ShortLivedProducersCount,
		ExpectedShortLivedConsumerTasks: shortLivedCount * clientCounts.ShortLivedConsumersCount,
	}
	expected.ExpectedShortLivedTasksCount = expected.ExpectedShortLivedProducerTasks + expected.ExpectedShortLivedConsumerTasks
	expected.ExpectedLongLivedTasksCount = expected.ExpectedLongLivedProducerTasks + expected.ExpectedLongLivedConsumerTasks
	expected.ExpectedTotalTasksCount = expected.ExpectedLongLivedTasksCount + expected.ExpectedShortLivedTasksCount

	return expected
}
