package soak_clients

import (
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/trogdor"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var logger log.Logger

var (
	trogdorCoordinatorHost = os.Getenv("TROGDOR_HOST")
	adminConfig            = trogdor.AdminConf{}
	adminConfPath          = os.Getenv("TROGDOR_ADMIN_CONF")
	trogdorAgentsCount, _  = strconv.Atoi(os.Getenv("TROGDOR_AGENTS_COUNT"))
	bootstrapServers       = os.Getenv("TROGDOR_BOOTSTRAPSERVERS")
	abortChannel           = make(chan abortError)
	abortGrp               = sync.WaitGroup{}
)

type abortError struct {
	message string
	taskIds []string
}

func (a *abortError) Error() string {
	return a.message
}

func InitLogger() {
	viper.BindEnv("DEBUG_LOGS")
	logger = log.NewJSONLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.Caller(4))
	logger = log.With(logger, "app", "soak-cluster-clients")
	if viper.GetBool("DEBUG_LOGS") {
		logger = level.NewFilter(logger, level.AllowDebug())
		logutil.Debug(logger, "enabled debug logs")
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}
}

func Run(topicConfigPath string) {
	InitLogger()
	err := adminConfig.ParseConfig(adminConfPath)
	if err != nil {
		logutil.Error(logger, "error while parsing admin config - %s", err)
		panic(err)
	}

	logutil.Info(logger, "starting up...")
	defer logutil.Info(logger, "shut down...")

	errc := make(chan error)
	// Interrupt handler.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	var runningTaskNames []string
	logutil.Info(logger, "Creating baseline tasks... (trogdor agents count: %d)", trogdorAgentsCount)
	taskSpecs, err := baselineTasks(topicConfigPath, trogdorAgentsCount)
	if err != nil {
		panic(err)
	}
	logutil.Info(logger, "Creating %d Trogdor tasks", len(runningTaskNames))
	for _, task := range taskSpecs {
		resp, body, err := task.CreateTask(trogdorCoordinatorHost)
		if err != nil {
			logutil.Error(logger, "Error while trying to create task %s. err: ", task.ID, err)
			abortAndCleanUp(abortError{err.Error(), runningTaskNames})
		}
		if code := resp.StatusCode; code > 299 {
			errMsg := fmt.Sprintf("Error while trying to create task %s. status code: %d, body: %s", task.ID, code, body)
			logutil.Error(logger, errMsg)
			abortAndCleanUp(abortError{errMsg, runningTaskNames})
		}
		logutil.Debug(logger, "Created task %s", task.ID)
		runningTaskNames = append(runningTaskNames, task.ID)
	}
	logutil.Info(logger, "Created %d Trogdor tasks", len(runningTaskNames))
}

// waits until all the existing tasks get cleaned up and panics
func abortAndCleanUp(err abortError) {
	abortGrp.Add(1)
	go abortTasks()
	abortChannel <- err

	logutil.Info(logger, "Aborting...")
	timedOut := waitTimeout(&abortGrp, time.Minute*5)
	if timedOut {
		logutil.Warn(logger, "Timed out waiting for task cleanup")
	}

	logutil.Error(logger, "Panicking... Error message: %s", err.Error())
	panic(err)
}

// Task cleanup in case of failure
func abortTasks() {
	err := <-abortChannel

	deletedTaskCount := 0
	logutil.Info(logger, "Beginning to delete %d tasks", len(err.taskIds))
	for _, taskId := range err.taskIds {
		resp, body, err := trogdor.DeleteTask(trogdorCoordinatorHost, taskId)
		if err != nil {
			logutil.Debug(logger, "error while trying to DELETE task %s. err: ", taskId, err)
			continue
		}
		if code := resp.StatusCode; code > 299 {
			logutil.Debug(logger, "error while trying to DELETE task %s. status code: %d, body: %s", taskId, code, body)
			continue
		}

		logutil.Debug(logger, "Successfully deleted task %s", taskId)
		deletedTaskCount += 1
	}
	logutil.Info(logger, "Successfully deleted %d tasks", len(err.taskIds))
	abortGrp.Done()
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
