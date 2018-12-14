package soak_clients

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/trogdor"
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
	topics := topicsConfig()
	produceCount, consumeCount := 0, 0
	for _, topic := range topics.Topics {
		consumeCount += topic.ConsumeCount
		produceCount += topic.ProduceCount
	}
	expected := calculateExpectedTasksCount(produceCount, consumeCount, topics.LongLivedTaskDurationMs, topics.ShortLivedTaskDurationMs)
	configPath := writeTopicsConfigFile(t, topics)
	tasks, err := baselineTasks(configPath, 10)
	if err != nil {
		fmt.Println(err)
		assert.Fail(t, "error while getting baseline tasks")
	}

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
	tasks := createTopicTasks(topicSpecification, oneWeekDurationMs, fifteenMinutesDurationMs/2, clientNodes)

	assertTaskCount(t, tasks, expected, oneWeekDurationMs, fifteenMinutesDurationMs/2)
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
	originalTopics := topicsConfig()
	topics := Topics{}
	fileName := writeTopicsConfigFile(t, originalTopics)
	err := topics.parseConfig(fileName)

	assert.NoError(t, err)
	assert.Equal(t, originalTopics, topics)
}

func writeTopicsConfigFile(t *testing.T, topics Topics) string {
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

func TestParseConfigReturnsErrorIfTopicNameNotWhitelisted(t *testing.T) {
	originalTopics := topicsConfig()
	originalTopics.Topics[0].Name = "invaliDddD"
	topics := Topics{}
	file, err := ioutil.TempFile("/tmp", "config")
	data, err := json.Marshal(originalTopics)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while marshalling topics %s", err))
	}
	file.Write(data)

	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while creating temporary file %s", err))
	}
	err = topics.parseConfig(file.Name())

	assert.Error(t, err)
}

func TestParseConfigReturnsErrorIfExtraTopicAdded(t *testing.T) {
	originalTopics := topicsConfig()
	originalTopics.Topics = append(originalTopics.Topics,
		TopicConfiguration{
			Name:                 "testTest",
			PartitionsCount:      1000,
			ProduceMBsThroughput: 60,
			ConsumeMBsThroughput: 60,
			ProduceCount:         10,
			ConsumeCount:         10,
		})
	topics := Topics{}
	file, err := ioutil.TempFile("/tmp", "config")
	data, err := json.Marshal(originalTopics)
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while marshalling topics %s", err))
	}
	file.Write(data)

	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while creating temporary file %s", err))
	}
	err = topics.parseConfig(file.Name())

	assert.Error(t, err)
}

func TestConsecutiveTasks(t *testing.T) {
	mediumTopic := trogdor.TopicSpec{
		NumPartitions:     16,
		ReplicationFactor: 3,
	}
	startConfig := &SoakScenarioConfig{
		scenarioId:         "ShortLivedConsumeMediumTopicTest",
		class:              trogdor.CONSUME_BENCH_SPEC_CLASS,
		agentCount:         3,
		topic:              mediumTopic,
		throughputMbPerSec: 7.5,
		consumerOptions:    trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		startMs:            10,
		durationMs:         5,
	}
	firstConfig := SoakScenarioConfig{}
	copier.Copy(&firstConfig, &startConfig)
	secondConfig := SoakScenarioConfig{}
	copier.Copy(&secondConfig, &startConfig)
	secondConfig.startMs = 15
	secondConfig.scenarioId = "ShortLivedConsumeMediumTopicTest-15"
	thirdConfig := SoakScenarioConfig{}
	copier.Copy(&thirdConfig, &startConfig)
	thirdConfig.startMs = 20
	thirdConfig.scenarioId = "ShortLivedConsumeMediumTopicTest-20"

	// should return 3 scenario configs
	expectedConfigs := []trogdor.ScenarioConfig{
		scenarioConfig(&firstConfig, clientNodes),
		scenarioConfig(&secondConfig, clientNodes),
		scenarioConfig(&thirdConfig, clientNodes),
	}

	actualConfigs, err := consecutiveTasks(startConfig, 25, clientNodes)
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
	startConfig := &SoakScenarioConfig{
		scenarioId:         "ShortLivedConsumeMediumTopicTest",
		class:              trogdor.CONSUME_BENCH_SPEC_CLASS,
		agentCount:         3,
		topic:              mediumTopic,
		throughputMbPerSec: 7.5,
		consumerOptions:    trogdor.ConsumerOptions{
			ConsumerGroup: "cg-1",
		},
		durationMs:         5,
	}
	_, err := consecutiveTasks(startConfig, 25, clientNodes)
	assert.Error(t, err)
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

func topicsConfig() Topics {
	topics := Topics{
		LongLivedTaskDurationMs:  oneWeekDurationMs,
		ShortLivedTaskDurationMs: fifteenMinutesDurationMs,
	}
	for _, whitelistName := range topicNameWhitelist {
		topics.Topics = append(topics.Topics, TopicConfiguration{
			Name:                 whitelistName,
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
