package soak_clients

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/trogdor"
	"time"
)

func Report(topicConfigPath string) {
	InitLogger()
	logutil.Info(logger, "Querying for tasks...")
	defer logutil.Info(logger, "Shutting down...")

	configuration := SoakTestConfig{}
	err := configuration.parseConfig(topicConfigPath)
	if err != nil {
		logutil.Error(logger, "error while parsing topic config - %s", err)
		panic(err)
	}

	nowMs := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	// Done, short-living tasks
	earliestStartMs := int64(nowMs - uint64(float64(configuration.ShortLivedTaskDurationMs)*1.5))
	latestStartMs := int64(nowMs - configuration.ShortLivedTaskDurationMs)
	doneTasks, err := trogdor.TaskStatuses(trogdorCoordinatorHost,
		earliestStartMs, latestStartMs, trogdor.DONE_TASK_STATE)
	printTasks(fmt.Sprintf("%s tasks, started between %d and %d", trogdor.DONE_TASK_STATE, earliestStartMs, latestStartMs),
		doneTasks, err)

	// Running tasks
	runningTasks, err := trogdor.TaskStatuses(trogdorCoordinatorHost, -1, -1, trogdor.RUNNING_TASK_STATE)
	printTasks(fmt.Sprintf("%s tasks", trogdor.RUNNING_TASK_STATE), runningTasks, err)

	// Stopping tasks
	stoppingTasks, err := trogdor.TaskStatuses(trogdorCoordinatorHost, -1, -1, trogdor.STOPPING_TASK_STATE)
	printTasks(fmt.Sprintf("%s tasks", trogdor.STOPPING_TASK_STATE), stoppingTasks, err)
}

func printTasks(tasksDescription string, taskStatuses map[string]*trogdor.TaskStatus, err error) {
	if err != nil {
		logutil.Error(logger, "error while fetching %s: %s", tasksDescription, err)
	} else {
		logutil.Info(logger, "Found %d %s", len(taskStatuses), tasksDescription)
		for taskId, task := range taskStatuses {
			logutil.Debug(logger, "Task %s: %+v", taskId, task)
			if task.Error != "" {
				logutil.Error(logger, "Task %s has an error - %s", taskId, task.Error)
				continue
			}

			jsonText, err := json.Marshal(&task.Status)
			if err != nil {
				logutil.Error(logger, "error while marshalling json for task %s: %s", taskId, err)
			} else {
				logutil.Info(logger, "Status for %s: %s", taskId, jsonText)
			}
		}
	}
}
