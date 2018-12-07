package performance

import (
	"fmt"
	"github.com/confluentinc/ce-kafka/cc-services/trogdor"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"strconv"
	"testing"
	"time"
)

var (
	trogdorCoordinatorHost       = os.Getenv("TROGDOR_HOST")
	trogdorTestTag               = os.Getenv("TROGDOR_TEST_TAG")
	trogdorAgentsCount, _        = strconv.Atoi(os.Getenv("TROGDOR_AGENTS_COUNT"))
	messagesStepPerCluster, _    = strconv.ParseUint(os.Getenv("TROGDOR_MESSAGES_INCREMENT"), 10, 64)
	minimumMessagesPerCluster, _ = strconv.ParseUint(os.Getenv("TROGDOR_MINIMUM_MESSAGES"), 10, 64)
	adminConfPath                = os.Getenv("TROGDOR_ADMIN_CONF")
	bootstrapServers             = os.Getenv("TROGDOR_BOOTSTRAPSERVERS")
	trogdorCoordinatorCreateURL  = fmt.Sprintf("%s/coordinator/task/create", trogdorCoordinatorHost)
	trogdorCoordinatorStatusURL  = fmt.Sprintf("%s/coordinator/status", trogdorCoordinatorHost)
	defaultTopicSpec             = trogdor.TopicSpec{NumPartitions: 256, ReplicationFactor: 3}
)


func clientNodes() []string {
	var clientNodes []string
	for agentID := 0; agentID < trogdorAgentsCount; agentID++ {
		// should be the same as the one in agentStatefulSet.yaml
		clientNodes = append(clientNodes, fmt.Sprintf("cc-trogdor-service-agent-%d", agentID))
	}
	return clientNodes
}


func TestProducerSaturation(t *testing.T) {

	Convey("When I create a progressive producer load scenario I should receive no errors", t, func() {

		adminConfig := trogdor.AdminConf{}
		adminConfig.ParseConfig(adminConfPath)
		scenarioConfig := trogdor.ScenarioConfig{ScenarioID: "LongProduceTest", AgentCount: trogdorAgentsCount,
			Class:                   "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
			TopicSpec:               defaultTopicSpec,
			LoopDurationMs:          600000,
			NumberOfLoops:           16,
			StartMs:                 uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			LoopCoolDownMs:          120000,
			BootstrapServers:        bootstrapServers,
			MinimumMessagesPerAgent: 8000,
			MessagesStepPerLoop:     80000,
			AdminConf:               adminConfig,
			ProducerOptions: trogdor.ProducerOptions{
				ValueGenerator: trogdor.DefaultValueGeneratorSpec,
				KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
			},
			ClientNodes: clientNodes(),
		}
		scenario := &trogdor.ScenarioSpec{}
		scenario.CreateScenario(scenarioConfig)
		fmt.Print(scenario.TaskSpecs)

		Convey("When I attempt to schedule the scenario I should receive no errors", func() {

			for _, v := range scenario.TaskSpecs {
				v.CreateTask(trogdorCoordinatorHost)
			}

		})

	})

}

func TestRoundTripSaturation(t *testing.T) {

	Convey("When I create a progressive RoundTrip load scenario I should receive no errors", t, func() {

		adminConfig := trogdor.AdminConf{}
		adminConfig.ParseConfig(adminConfPath)
		scenarioConfig := trogdor.ScenarioConfig{ScenarioID: "LongRoundTripTest", AgentCount: trogdorAgentsCount,
			Class:                   "org.apache.kafka.trogdor.workload.RoundTripWorkloadSpec",
			TopicSpec:               defaultTopicSpec,
			LoopDurationMs:          600000,
			NumberOfLoops:           16,
			StartMs:                 uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			LoopCoolDownMs:          120000,
			BootstrapServers:        bootstrapServers,
			MinimumMessagesPerAgent: 8000,
			MessagesStepPerLoop:     80000,
			AdminConf:               adminConfig,
			ProducerOptions: trogdor.ProducerOptions{
				ValueGenerator: trogdor.DefaultValueGeneratorSpec,
				KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
			},
			ClientNodes: clientNodes(),
		}
		scenario := &trogdor.ScenarioSpec{}
		scenario.CreateScenario(scenarioConfig)
		fmt.Print(scenario.TaskSpecs)

		Convey("When I attempt to schedule the scenario I should receive no errors", func() {

			for _, v := range scenario.TaskSpecs {
				v.CreateTask(trogdorCoordinatorHost)
			}

		})

	})

}

func TestAuthSaturation(t *testing.T) {

	Convey("We are attempting to simulate a lot of clients hammering auth", t, func() {

		adminConfig := trogdor.AdminConf{}
		adminConfig.ParseConfig(adminConfPath)
		scenarioConfig := trogdor.ScenarioConfig{ScenarioID: "LongProduceTest", AgentCount: trogdorAgentsCount,
			Class:                   "org.apache.kafka.trogdor.workload.ProduceBenchSpec",
			TopicSpec:               defaultTopicSpec,
			LoopDurationMs:          500,
			NumberOfLoops:           1000,
			StartMs:                 uint64(time.Now().UnixNano() / int64(time.Millisecond)),
			LoopCoolDownMs:          10,
			BootstrapServers:        bootstrapServers,
			MinimumMessagesPerAgent: 500,
			MessagesStepPerLoop:     0,
			AdminConf:               adminConfig,
			ProducerOptions:         trogdor.ProducerOptions{
				ValueGenerator: trogdor.DefaultValueGeneratorSpec,
				KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
			},
			ClientNodes: clientNodes(),
		}
		scenario := &trogdor.ScenarioSpec{}
		scenario.CreateScenario(scenarioConfig)

		for _, v := range scenario.TaskSpecs {
			v.CreateTask(trogdorCoordinatorHost)
		}

		Convey("Schedule a steady load to see when failure is reached", func() {

			adminConfig := trogdor.AdminConf{}
			adminConfig.ParseConfig(adminConfPath)
			scenarioConfig := trogdor.ScenarioConfig{ScenarioID: "LongProduceTest", AgentCount: trogdorAgentsCount,
				TopicSpec:               defaultTopicSpec,
				LoopDurationMs:          50000,
				NumberOfLoops:           16,
				StartMs:                 uint64(time.Now().UnixNano() / int64(time.Millisecond)),
				LoopCoolDownMs:          10,
				BootstrapServers:        bootstrapServers,
				MinimumMessagesPerAgent: 100000,
				MessagesStepPerLoop:     0,
				AdminConf:               adminConfig,
				ProducerOptions: trogdor.ProducerOptions{
					ValueGenerator: trogdor.DefaultValueGeneratorSpec,
					KeyGenerator:   trogdor.DefaultKeyGeneratorSpec,
				},
				ClientNodes: clientNodes(),
			}


			scenario := &trogdor.ScenarioSpec{}
			scenario.CreateScenario(scenarioConfig)

			for _, v := range scenario.TaskSpecs {
				resp, body, err := v.CreateTask(trogdorCoordinatorHost)
				if err != nil {
					print(err)
				} else {
					print(resp.Body, body)
				}

			}

		})

	})
}
