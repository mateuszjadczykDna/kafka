package trogdor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type AdminConf struct {
	CompressionType                    string `json:"compression.type"`
	Acks                               string `json:"acks"`
	LingerMs                           int64  `json:"linger.ms"`
	MaxInFlightRequestsPerConnection   uint64 `json:"max.in.flight.requests.per.connection"`
	RetryBackoffMs                     int64  `json:"retry.backoff.ms,omitempty"`
	SaslJaasConfig                     string `json:"sasl.jaas.config,omitempty"`
	SecurityProtocol                   string `json:"security.protocol,omitempty"`
	SslEndpointIdentificationAlgorithm string `json:"ssl.endpoint.identification.algorithm,omitempty"`
	SaslMechanism                      string `json:"sasl.mechanism,omitempty"`
	SslProtocol                        string `json:"ssl.protocol,omitempty"`
	SslKeyPassword                     string `json:"ssl.key.password,omitempty"`
	SslKeystoreLocation                string `json:"ssl.keystore.location,omitempty"`
	SslKeystorePassword                string `json:"ssl.keystore.password,omitempty"`
	SslKeystoreType                    string `json:"ssl.keystore.type,omitempty"`
	SslTruststoreLocation              string `json:"ssl.truststore.location,omitempty"`
	SslTruststorePassword              string `json:"ssl.truststore.password,omitempty"`
	SslTruststoreType                  string `json:"ssl.truststore.type,omitempty"`
}

type StatusSpec struct {
	ServerStartMs uint64 `json:"serverStartMs"`
}

type producerSpec struct {
	Class                string             `json:"class"`
	StartMs              uint64             `json:"startMs"`
	DurationMs           uint64             `json:"durationMs"`
	ProducerNode         string             `json:"producerNode,omitempty"`
	BootstrapServers     string             `json:"bootstrapServers"`
	TargetMessagesPerSec uint64             `json:"targetMessagesPerSec"`
	MaxMessages          uint64             `json:"maxMessages"`
	KeyGenerator         KeyGeneratorSpec   `json:"keyGenerator"`
	ValueGenerator       ValueGeneratorSpec `json:"valueGenerator"`
	ProducerConf         *AdminConf         `json:"producerConf,omitempty"`
	CommonClientConf     *AdminConf         `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf         `json:"adminClientConf,omitempty"`
	ActiveTopics         json.RawMessage    `json:"activeTopics"`
}

type roundTripSpec struct {
	Class                string             `json:"class"`
	StartMs              uint64             `json:"startMs"`
	DurationMs           uint64             `json:"durationMs"`
	ClientNode           string             `json:"clientNode,omitempty"`
	BootstrapServers     string             `json:"bootstrapServers"`
	TargetMessagesPerSec uint64             `json:"targetMessagesPerSec"`
	MaxMessages          uint64             `json:"maxMessages"`
	ValueGenerator       ValueGeneratorSpec `json:"valueGenerator"`
	ProducerConf         *AdminConf         `json:"producerConf,omitempty"`
	CommonClientConf     *AdminConf         `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf         `json:"adminClientConf,omitempty"`
	ActiveTopics         json.RawMessage    `json:"activeTopics"`
}

type TaskSpec struct {
	ID   string          `json:"id"`
	Spec json.RawMessage `json:"spec"`
}

type ValueGeneratorSpec struct {
	ValueType string `json:"type"`
	Size      uint64 `json:"size"`
	Padding   uint64 `json:"padding"`
}

type KeyGeneratorSpec struct {
	Type        string `json:"type"`
	Size        uint64 `json:"size"`
	StartOffset uint64 `json:"startOffset"`
}

type TopicSpec struct {
	NumPartitions     uint64 `json:"numPartitions"`
	ReplicationFactor uint64 `json:"replicationFactor"`
}

type ScenarioSpec struct {
	TaskSpecs []TaskSpec
}

type ScenarioConfig struct {
	ScenarioID                string
	Class                     string
	AgentCount                int
	TotalTopics               uint64
	ActiveTopics              TopicSpec
	InactiveTopics            TopicSpec
	LoopDurationMs            uint64
	StartMs                   uint64
	NumberOfLoops             int
	LoopCoolDownMs            uint64
	BootstrapServers          string
	MinimumMessagesPerCluster uint64
	MessagesStepPerCluster    uint64
	AdminConf                 AdminConf
	ValueGenerator            ValueGeneratorSpec
	KeyGenerator              KeyGeneratorSpec
}

func (r *AdminConf) ParseConfig(adminConfFile string) error {
	raw, err := ioutil.ReadFile(adminConfFile)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(raw, &r)

	return err

}

func (r *TopicSpec) ReturnTopicSpecRawJson(topicString string) json.RawMessage {
	raw := json.RawMessage{}
	jsonString := fmt.Sprintf(`{"%s": { "numPartitions": %d, "replicationFactor": %d}}`, topicString, r.NumPartitions, r.ReplicationFactor)
	json.Unmarshal([]byte(jsonString), &raw)

	return raw
}

func (r *ScenarioSpec) CreateScenario(scenarioConfig ScenarioConfig) {
	scenario := []TaskSpec{}
	rawSpec := json.RawMessage{}

	for loadLoop := 0; loadLoop < scenarioConfig.NumberOfLoops; loadLoop++ {

		for agentID := 0; agentID < scenarioConfig.AgentCount; agentID++ {

			switch class := scenarioConfig.Class; class {

			case "org.apache.kafka.trogdor.workload.RoundTripWorkloadSpec":
				{
					//Due to roundTrip wanting dedicated topics per agent and loop.
					topicString := fmt.Sprintf("TrogdorTopicLoop%dAgent%d[1-1]", loadLoop, agentID)
					activeTopics := scenarioConfig.ActiveTopics.ReturnTopicSpecRawJson(topicString)

					roundTripWorkloadSpecData := roundTripSpec{
						Class:            scenarioConfig.Class,
						StartMs:          scenarioConfig.StartMs,
						DurationMs:       scenarioConfig.LoopDurationMs,
						ClientNode:       fmt.Sprintf("trogdoragent-%d", agentID),
						BootstrapServers: scenarioConfig.BootstrapServers,
						//Remember that target messages per sec is actually per topic.
						TargetMessagesPerSec: scenarioConfig.MinimumMessagesPerCluster + (scenarioConfig.MessagesStepPerCluster*uint64(loadLoop))/uint64(scenarioConfig.AgentCount),
						MaxMessages:          (scenarioConfig.MinimumMessagesPerCluster + (scenarioConfig.MessagesStepPerCluster*uint64(loadLoop))/uint64(scenarioConfig.AgentCount)) * (uint64(scenarioConfig.LoopDurationMs) / 1000),
						ValueGenerator:       scenarioConfig.ValueGenerator,
						CommonClientConf:     &scenarioConfig.AdminConf,
						ActiveTopics:         activeTopics,
					}
					rawSpec, _ = json.Marshal(roundTripWorkloadSpecData)

				}

			case "org.apache.kafka.trogdor.workload.ProduceBenchSpec":
				{
					activeTopics := scenarioConfig.ActiveTopics.ReturnTopicSpecRawJson("TrogdorTopic[1-1]")

					produceBenchSpecData := producerSpec{
						Class:            scenarioConfig.Class,
						StartMs:          scenarioConfig.StartMs,
						DurationMs:       scenarioConfig.LoopDurationMs,
						ProducerNode:     fmt.Sprintf("trogdoragent-%d", agentID),
						BootstrapServers: scenarioConfig.BootstrapServers,
						//Remember that target messages per sec is actually per topic.
						TargetMessagesPerSec: scenarioConfig.MinimumMessagesPerCluster + (scenarioConfig.MessagesStepPerCluster*uint64(loadLoop))/uint64(scenarioConfig.AgentCount),
						MaxMessages:          (scenarioConfig.MinimumMessagesPerCluster + (scenarioConfig.MessagesStepPerCluster*uint64(loadLoop))/uint64(scenarioConfig.AgentCount)) * (uint64(scenarioConfig.LoopDurationMs) / 1000),
						ValueGenerator:       scenarioConfig.ValueGenerator,
						KeyGenerator:         scenarioConfig.KeyGenerator,
						CommonClientConf:     &scenarioConfig.AdminConf,
						ActiveTopics:         activeTopics,
					}
					rawSpec, _ = json.Marshal(produceBenchSpecData)
				}

			}

			SpecData := TaskSpec{
				ID:   fmt.Sprintf("producerSaturation-loadLoop%d-agentID%d-testTag%s", loadLoop, agentID, scenarioConfig.ScenarioID),
				Spec: rawSpec,
			}
			scenario = append(scenario, SpecData)
		}
		scenarioConfig.StartMs = scenarioConfig.StartMs + scenarioConfig.LoopCoolDownMs + scenarioConfig.LoopDurationMs
	}
	r.TaskSpecs = scenario
}

func (r *TaskSpec) SendRequest(coordinatorURL string) (*http.Response, []byte, error) {

	out, err := json.Marshal(r)

	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/coordinator/task/create", coordinatorURL), bytes.NewBuffer(out))
	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		return nil, nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	return resp, body, nil
}
