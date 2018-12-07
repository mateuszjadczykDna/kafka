package trogdor

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var sampleTasksReport = fmt.Sprintf(`
{
  "tasks" : {
    "consume_bench_5141" : %s,
    "produce_bench_13473" : %s
  }
}
`, sampleConsumeTaskReport, sampleProduceTaskReport)

var sampleConsumeTaskReport = fmt.Sprintf(`
{
      "state" : "DONE",
      "spec" : {
        "class" : "org.apache.kafka.trogdor.workload.ConsumeBenchSpec",
        "startMs" : 0,
        "durationMs" : 10000000,
        "consumerNode" : "node0",
        "bootstrapServers" : "localhost:9092",
        "targetMessagesPerSec" : 1000,
        "maxMessages" : 10000,
        "consumerGroup" : "cg",
        "threadsPerWorker" : 5,
        "activeTopics" : [ "foo[1-3]" ]
      },
      "startedMs" : 1542023879901,
      "doneMs" : 1542023890950,
      "cancelled" : false,
      "status" : %s
    }
`, sampleConsumeTaskStatus)

var sampleConsumeTaskStatus = `
{
  "consumer.consume_bench_5141-0" : {
    "assignedPartitions" : [ "foo3-0", "foo3-1", "foo2-0", "foo2-1", "foo1-0", "foo1-1" ],
    "totalMessagesReceived" : 10000,
    "totalBytesReceived" : 5160000,
    "averageMessageSizeBytes" : 516,
    "averageLatencyMs" : 12.6,
    "p50LatencyMs" : 2,
    "p95LatencyMs" : 5,
    "p99LatencyMs" : 213
  },
  "consumer.consume_bench_5141-4" : {
    "assignedPartitions" : [ "foo3-8", "foo3-9", "foo2-8", "foo2-9", "foo1-8", "foo1-9" ],
    "totalMessagesReceived" : 10000,
    "totalBytesReceived" : 5160000,
    "averageMessageSizeBytes" : 516,
    "averageLatencyMs" : 12.3,
    "p50LatencyMs" : 1,
    "p95LatencyMs" : 4,
    "p99LatencyMs" : 210
  },
  "consumer.consume_bench_5141-2" : {
    "assignedPartitions" : [ "foo3-4", "foo3-5", "foo2-4", "foo2-5", "foo1-4", "foo1-5" ],
    "totalMessagesReceived" : 10000,
    "totalBytesReceived" : 5160000,
    "averageMessageSizeBytes" : 516,
    "averageLatencyMs" : 12.5,
    "p50LatencyMs" : 1,
    "p95LatencyMs" : 5,
    "p99LatencyMs" : 214
  },
  "consumer.consume_bench_5141-3" : {
    "assignedPartitions" : [ "foo3-6", "foo3-7", "foo2-6", "foo2-7", "foo1-6", "foo1-7" ],
    "totalMessagesReceived" : 10000,
    "totalBytesReceived" : 5160000,
    "averageMessageSizeBytes" : 516,
    "averageLatencyMs" : 12.35,
    "p50LatencyMs" : 2,
    "p95LatencyMs" : 5,
    "p99LatencyMs" : 212
  }
}
`

var sampleProduceTaskReport = fmt.Sprintf(`
{
      "state" : "DONE",
      "spec" : {
        "class" : "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
        "startMs" : 0,
        "durationMs" : 10000000,
        "producerNode" : "node0",
        "bootstrapServers" : "localhost:9092",
        "targetMessagesPerSec" : 10000,
        "maxMessages" : 50000,
        "keyGenerator" : {
          "type" : "sequential",
          "size" : 4,
          "startOffset" : 0
        },
        "valueGenerator" : {
          "type" : "constant",
          "size" : 512,
          "value" : "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        },
        "activeTopics" : {
          "foo[1-3]" : {
            "numPartitions" : 10,
            "replicationFactor" : 1
          }
        },
        "inactiveTopics" : {
          "foo[4-5]" : {
            "numPartitions" : 10,
            "replicationFactor" : 1
          }
        }
      },
      "startedMs" : 1542022826821,
      "doneMs" : 1542022833845,
      "cancelled" : false,
      "status" : %s 
    }
`, sampleProduceTaskStatus)

var sampleProduceTaskStatus = `
{
  "totalSent" : 50000,
  "averageLatencyMs" : 6.2655,
  "p50LatencyMs" : 6,
  "p95LatencyMs" : 11,
  "p99LatencyMs" : 14
}
`

func TestParseStatuses(t *testing.T) {
	taskStatuses, err := parseStatuses(strings.NewReader(sampleTasksReport))
	if err != nil {
		assert.Fail(t, fmt.Sprintf("error while parsing statuses %s", err))
	}

	produceStatus := taskStatuses["produce_bench_13473"]
	assert.Equal(t, PRODUCE_BENCH_SPEC_CLASS, produceStatus.Spec.Class)
	assert.Equal(t, DONE_TASK_STATE, produceStatus.State)
	verifyProduceTaskStatus(t, produceStatus)

	consumeStatus := taskStatuses["consume_bench_5141"]
	assert.Equal(t, CONSUME_BENCH_SPEC_CLASS, consumeStatus.Spec.Class)
	assert.Equal(t, DONE_TASK_STATE, consumeStatus.State)
	verifyConsumeTaskStatus(t, consumeStatus)
}

func TestParseStatus(t *testing.T) {
	status := taskReport(t, sampleProduceTaskReport)
	assert.True(t, status.isProduceTask())
	assert.Equal(t, PRODUCE_BENCH_SPEC_CLASS, status.Spec.Class)
	assert.Equal(t, DONE_TASK_STATE, status.State)
	assert.False(t, status.Cancelled)
	assert.Equal(t, "", status.Error)
	verifyProduceTaskStatus(t, status)
}

func TestProduceTaskStatus(t *testing.T) {
	status := taskReport(t, sampleProduceTaskReport)
	verifyProduceTaskStatus(t, status)
}

func TestConsumeTaskStatus(t *testing.T) {
	verifyConsumeTaskStatus(t, taskReport(t, sampleConsumeTaskReport))
}

func TestConsumeTaskStatusReturnsErrorIfNotConsumeStatus(t *testing.T) {
	status := taskReport(t, sampleProduceTaskReport)
	_, err := status.consumeTaskStatus()
	assert.Error(t, err)
}

func TestProduceTaskStatusReturnsErrorIfNotProduceStatus(t *testing.T) {
	status := taskReport(t, sampleConsumeTaskReport)
	_, err := status.produceTaskStatus()
	assert.Error(t, err)
}

func taskReport(t *testing.T, reportJson string) *TaskStatus {
	spec := TaskSpec{}
	status, err := spec.parseStatus(strings.NewReader(reportJson))
	if err != nil {
		assert.Fail(t, "error while parsing status %s", err)
	}
	return status
}

func verifyProduceTaskStatus(t *testing.T, status *TaskStatus) {
	taskStatus, err := status.produceTaskStatus()
	if err != nil {
		assert.Fail(t, "error while parsing produce task status %s", err)
	}
	assert.Equal(t, 6.2655, taskStatus.AverageLatencyMs)
	assert.Equal(t, 50000, taskStatus.TotalSent)
	assert.Equal(t, 6, taskStatus.P50LatencyMs)
	assert.Equal(t, 11, taskStatus.P95LatencyMs)
	assert.Equal(t, 14, taskStatus.P99LatencyMs)
}

func verifyConsumeTaskStatus(t *testing.T, status *TaskStatus) {
	taskStatuses, err := status.consumeTaskStatus()
	if err != nil {
		assert.Fail(t, "error while parsing consume task status %s", err)
	}
	fourthConsumer := taskStatuses["consumer.consume_bench_5141-4"]
	assert.Equal(t,
		[]string{"foo3-0", "foo3-1", "foo2-0", "foo2-1", "foo1-0", "foo1-1"},
		taskStatuses["consumer.consume_bench_5141-0"].AssignedPartitions,
	)
	assert.Equal(t,
		[]string{"foo3-4", "foo3-5", "foo2-4", "foo2-5", "foo1-4", "foo1-5"},
		taskStatuses["consumer.consume_bench_5141-2"].AssignedPartitions,
	)
	assert.Equal(t,
		[]string{"foo3-6", "foo3-7", "foo2-6", "foo2-7", "foo1-6", "foo1-7"},
		taskStatuses["consumer.consume_bench_5141-3"].AssignedPartitions,
	)
	assert.Equal(t,
		[]string{"foo3-8", "foo3-9", "foo2-8", "foo2-9", "foo1-8", "foo1-9"},
		fourthConsumer.AssignedPartitions,
	)
	assert.Equal(t, 10000, fourthConsumer.TotalMessagesReceived)
	assert.Equal(t, 516, fourthConsumer.AverageMessageSizeBytes)
	assert.Equal(t, 12.3, fourthConsumer.AverageLatencyMs)
	assert.Equal(t, 1, fourthConsumer.P50LatencyMs)
	assert.Equal(t, 4, fourthConsumer.P95LatencyMs)
	assert.Equal(t, 210, fourthConsumer.P99LatencyMs)
}
