package soak_clients

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"
	"testing"
)

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

var sampleProduceTaskStatus = `
{
  "totalSent" : 50000,
  "averageLatencyMs" : 6.2655,
  "p50LatencyMs" : 6,
  "p95LatencyMs" : 11,
  "p99LatencyMs" : 14
}
`

func TestPrintTasksPrintsError(t *testing.T) {
	logger = common.InitLogger("soak-cluster-clients")
	tasks := map[string]*trogdor.TaskStatus{
		"task": {
			State: "RUNNING",
		},
	}
	printTasks("5 RUNNING tasks", tasks, errors.New("returned 500"))
}

func TestPrintTasksPrintsTasksWithError(t *testing.T) {
	logger = common.InitLogger("soak-cluster-clients")
	tasks := map[string]*trogdor.TaskStatus{
		"task": {
			State: "RUNNING",
			Error: "NullPointerException",
		},
	}
	printTasks("5 RUNNING tasks", tasks, nil)
}

func TestPrintTasksPrintsTasks(t *testing.T) {
	logger = common.InitLogger("soak-cluster-clients")
	tasks := map[string]*trogdor.TaskStatus{
		"consume_task_1": {
			State:  "RUNNING",
			Status: []byte(sampleConsumeTaskStatus),
		},
		"produce_task_1": {
			State:  "RUNNING",
			Status: []byte(sampleProduceTaskStatus),
		},
	}
	printTasks("RUNNING tasks", tasks, nil)
}
