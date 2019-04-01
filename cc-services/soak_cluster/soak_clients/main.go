package soak_clients

import (
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/go-kit/kit/log"

	"os"
	"os/signal"
	"syscall"
)

var logger log.Logger

var (
	adminConfig   = trogdor.AdminConf{}
	adminConfPath = os.Getenv("TROGDOR_ADMIN_CONF")
)

func Run(topicConfigPath string, trogdorCoordinatorHost string, trogdorAgentsCount int, bootstrapServers string) {
	logger = common.InitLogger("soak-cluster-clients")
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

	logutil.Info(logger, "Creating baseline tasks... (trogdor agents count: %d)", trogdorAgentsCount)
	taskSpecs, err := baselineTasks(topicConfigPath, trogdorAgentsCount, bootstrapServers)
	if err != nil {
		panic(err)
	}
	common.ScheduleTrogdorTasks(logger, taskSpecs, trogdorCoordinatorHost)
}
