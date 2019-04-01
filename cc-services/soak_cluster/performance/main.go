package performance

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
)

var logger log.Logger

type PerformanceTestConfig struct {
	Type       string          `json:"test_type"`
	Name       string          `json:"test_name"`
	Parameters json.RawMessage `json:"test_parameters"`
}

const PROGRESSIVE_WORKLOAD_TEST_TYPE = "ProgressiveWorkload"

func (performanceTestConfig *PerformanceTestConfig) parseConfig(configPath string) error {
	raw, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed reading the performance test configuration from %s", configPath))
	}

	err = json.Unmarshal(raw, &performanceTestConfig)
	if err != nil {
		return err
	}

	logutil.Info(logger, "Loaded configuration for test %s of type %s.", performanceTestConfig.Name, performanceTestConfig.Type)
	return nil
}

var (
	adminConfig   = trogdor.AdminConf{}
	adminConfPath = os.Getenv("TROGDOR_ADMIN_CONF")
)

func Run(testConfigPath string, trogdorCoordinatorHost string, trogdorAgentsCount int, bootstrapServers string) {
	logger = common.InitLogger("performance-tests")
	err := adminConfig.ParseConfig(adminConfPath)
	if err != nil {
		logutil.Error(logger, "error while parsing admin config - %s", err)
		panic(err)
	}

	testConfig := PerformanceTestConfig{}
	err = testConfig.parseConfig(testConfigPath)
	if err != nil {
		logutil.Error(logger, "error while parsing performance test config - %s", err)
		panic(err)
	}
	var tasks []trogdor.TaskSpec
	switch testConfig.Type {
	case PROGRESSIVE_WORKLOAD_TEST_TYPE:
		var progressiveWorkload Workload
		err = json.Unmarshal(testConfig.Parameters, &progressiveWorkload)
		if err != nil {
			panic(errors.Wrapf(err, "error while trying to parse progressive workload test parameters"))
		}
		progressiveWorkload.Name = testConfig.Name
		tasks, err = progressiveWorkload.CreateWorkload(trogdorAgentsCount, bootstrapServers)
		if err != nil {
			panic(err)
		}
	default:
		panic(errors.New(fmt.Sprintf("test type %s is not supported", testConfig.Type)))
	}

	common.ScheduleTrogdorTasks(logger, tasks, trogdorCoordinatorHost)
}
