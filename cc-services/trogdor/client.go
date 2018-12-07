package trogdor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type AdminConf struct {
	CompressionType                    string `json:"compression.type,omitempty"`
	Acks                               string `json:"acks,omitempty"`
	EnableIdempotence                  string `json:"enable.idempotence,omitempty"`
	LingerMs                           int64  `json:"linger.ms,omitempty"`
	MaxInFlightRequestsPerConnection   uint64 `json:"max.in.flight.requests.per.connection,omitempty"`
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

type producerSpec struct {
	Class                string                    `json:"class"`
	StartMs              uint64                    `json:"startMs"`
	DurationMs           uint64                    `json:"durationMs"`
	ProducerNode         string                    `json:"producerNode,omitempty"`
	BootstrapServers     string                    `json:"bootstrapServers"`
	TargetMessagesPerSec uint64                    `json:"targetMessagesPerSec"`
	MaxMessages          uint64                    `json:"maxMessages"`
	KeyGenerator         KeyGeneratorSpec          `json:"keyGenerator"`
	ValueGenerator       ValueGeneratorSpec        `json:"valueGenerator"`
	TransactionGenerator *TransactionGeneratorSpec `json:"transactionGenerator,omitempty"`
	ProducerConf         *AdminConf                `json:"producerConf,omitempty"`
	CommonClientConf     *AdminConf                `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf                `json:"adminClientConf,omitempty"`
	ActiveTopics         json.RawMessage           `json:"activeTopics"`
}

type consumerSpec struct {
	Class                string     `json:"class"`
	StartMs              uint64     `json:"startMs"`
	DurationMs           uint64     `json:"durationMs"`
	ConsumerNode         string     `json:"consumerNode,omitempty"`
	ConsumerGroup        string     `json:"consumerGroup,omitempty"`
	BootstrapServers     string     `json:"bootstrapServers"`
	TargetMessagesPerSec uint64     `json:"targetMessagesPerSec"`
	MaxMessages          uint64     `json:"maxMessages"`
	ConsumerConf         *AdminConf `json:"consumerConf,omitempty"`
	CommonClientConf     *AdminConf `json:"commonClientConf,omitempty"`
	AdminClientConf      *AdminConf `json:"adminClientConf,omitempty"`
	ActiveTopics         []string   `json:"activeTopics"`
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

type taskStatusSpec struct {
	Class string `json:"class"`
}

type TaskStatus struct {
	State     string          `json:"state"`
	Spec      taskStatusSpec  `json:"spec"`
	StartedMs int64           `json:"startedMs"`
	DoneMs    int64           `json:"doneMs"`
	Cancelled bool            `json:"cancelled"`
	Error     string          `json:"error"`
	Status    json.RawMessage `json:"status"`
}

type produceTaskStatus struct {
	TotalSent        int     `json:"totalSent"`
	AverageLatencyMs float64 `json:"averageLatencyMs"`
	P50LatencyMs     int     `json:"p50LatencyMs"`
	P95LatencyMs     int     `json:"p95LatencyMs"`
	P99LatencyMs     int     `json:"p99LatencyMs"`
}

type consumeTaskStatus struct {
	AssignedPartitions      []string `json:"assignedPartitions"`
	TotalMessagesReceived   int      `json:"totalMessagesReceived"`
	TotalBytesReceived      int      `json:"totalBytesReceived"`
	AverageMessageSizeBytes int      `json:"averageMessageSizeBytes"`
	AverageLatencyMs        float64  `json:"averageLatencyMs"`
	P50LatencyMs            int      `json:"p50LatencyMs"`
	P95LatencyMs            int      `json:"p95LatencyMs"`
	P99LatencyMs            int      `json:"p99LatencyMs"`
}

const (
	PENDING_TASK_STATE  = "PENDING"
	RUNNING_TASK_STATE  = "RUNNING"
	STOPPING_TASK_STATE = "STOPPING"
	DONE_TASK_STATE     = "DONE"
)

type ValueGeneratorSpec struct {
	ValueType string `json:"type"`
	Size      uint64 `json:"size"`
	Padding   uint64 `json:"padding"`
}

type TransactionGeneratorSpec struct {
	Type                   string `json:"type"`
	MessagesPerTransaction uint64 `json:"messagesPerTransaction"`
}

// use mostly random values to best simulate real-life data compression
var DefaultValueGeneratorSpec = ValueGeneratorSpec{ValueType: "uniformRandom", Size: 900, Padding: 100}
var DefaultKeyGeneratorSpec = KeyGeneratorSpec{Type: "sequential", Size: 4, StartOffset: 0}
var DefaultTransactionGeneratorSpec = TransactionGeneratorSpec{Type: "uniform", MessagesPerTransaction: 100}

type KeyGeneratorSpec struct {
	Type        string `json:"type"`
	Size        uint64 `json:"size"`
	StartOffset uint64 `json:"startOffset"`
}

type TopicSpec struct {
	NumPartitions     uint64 `json:"numPartitions"`
	ReplicationFactor uint64 `json:"replicationFactor"`
	TopicName         string
}

type ScenarioSpec struct {
	TaskSpecs []TaskSpec
}

type ConsumerOptions struct {
	ConsumerGroup string
}

type ProducerOptions struct {
	ValueGenerator       ValueGeneratorSpec
	TransactionGenerator TransactionGeneratorSpec
	KeyGenerator         KeyGeneratorSpec
}

type ScenarioConfig struct {
	ScenarioID              string
	Class                   string
	AgentCount              int
	TotalTopics             uint64
	TopicSpec               TopicSpec
	LoopDurationMs          uint64
	StartMs                 uint64
	NumberOfLoops           int // setting this to 0 means that we will run one single loop with 1x loop multiplier
	LoopCoolDownMs          uint64
	BootstrapServers        string
	MinimumMessagesPerAgent uint64 // the minimum amount of messages we want each task to consume/produce
	MessagesStepPerLoop     uint64
	AdminConf               AdminConf
	ProducerOptions         ProducerOptions
	ConsumerOptions         ConsumerOptions
	ClientNodes             []string // all the configured trogdor nodes
}

const PRODUCE_BENCH_SPEC_CLASS = "org.apache.kafka.trogdor.workload.ProduceBenchSpec"
const CONSUME_BENCH_SPEC_CLASS = "org.apache.kafka.trogdor.workload.ConsumeBenchSpec"

func (r *AdminConf) ParseConfig(adminConfFile string) error {
	raw, err := ioutil.ReadFile(adminConfFile)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(raw, &r)

	return err

}

func (r *TopicSpec) ReturnTopicSpecRawJson() json.RawMessage {
	raw := json.RawMessage{}
	jsonString := fmt.Sprintf(`{"%s": { "numPartitions": %d, "replicationFactor": %d}}`, r.TopicName, r.NumPartitions, r.ReplicationFactor)
	json.Unmarshal([]byte(jsonString), &raw)

	return raw
}

func (r *ScenarioSpec) createAgentTasks(scenarioConfig ScenarioConfig, scenario []TaskSpec, loadLoop int) []TaskSpec {
	rawSpec := json.RawMessage{}

	clientNodesCount := len(scenarioConfig.ClientNodes)
	for agentIdx := 0; agentIdx < scenarioConfig.AgentCount; agentIdx++ {

		messagesStepPerAgent := (scenarioConfig.MessagesStepPerLoop * uint64(loadLoop)) / uint64(scenarioConfig.AgentCount)
		messagesPerSec := scenarioConfig.MinimumMessagesPerAgent + messagesStepPerAgent
		durationSeconds := uint64(scenarioConfig.LoopDurationMs) / 1000
		maxMessages := durationSeconds * messagesPerSec

		clientNode := scenarioConfig.ClientNodes[agentIdx%clientNodesCount]
		switch class := scenarioConfig.Class; class {

		case "org.apache.kafka.trogdor.workload.RoundTripWorkloadSpec":
			{
				//Due to roundTrip wanting dedicated topics per agent and loop.
				scenarioConfig.TopicSpec.TopicName = fmt.Sprintf("TrogdorTopicLoop%dClientNode%s[1-1]", loadLoop, clientNode)
				activeTopics := scenarioConfig.TopicSpec.ReturnTopicSpecRawJson()

				roundTripWorkloadSpecData := roundTripSpec{
					Class:                scenarioConfig.Class,
					StartMs:              scenarioConfig.StartMs,
					DurationMs:           scenarioConfig.LoopDurationMs,
					ClientNode:           clientNode,
					BootstrapServers:     scenarioConfig.BootstrapServers,
					TargetMessagesPerSec: messagesPerSec,
					MaxMessages:          maxMessages,
					ValueGenerator:       scenarioConfig.ProducerOptions.ValueGenerator,
					CommonClientConf:     &scenarioConfig.AdminConf,
					ActiveTopics:         activeTopics,
				}
				rawSpec, _ = json.Marshal(roundTripWorkloadSpecData)

			}

		case PRODUCE_BENCH_SPEC_CLASS:
			{
				if scenarioConfig.TopicSpec.TopicName == "" {
					scenarioConfig.TopicSpec.TopicName = "TrogdorTopic[1-1]"
				}
				activeTopics := scenarioConfig.TopicSpec.ReturnTopicSpecRawJson()

				produceBenchSpecData := producerSpec{
					Class:                scenarioConfig.Class,
					StartMs:              scenarioConfig.StartMs,
					DurationMs:           scenarioConfig.LoopDurationMs,
					ProducerNode:         clientNode,
					BootstrapServers:     scenarioConfig.BootstrapServers,
					TargetMessagesPerSec: messagesPerSec,
					MaxMessages:          maxMessages,
					ValueGenerator:       scenarioConfig.ProducerOptions.ValueGenerator,
					KeyGenerator:         scenarioConfig.ProducerOptions.KeyGenerator,
					CommonClientConf:     &scenarioConfig.AdminConf,
					ActiveTopics:         activeTopics,
				}
				if scenarioConfig.ProducerOptions.TransactionGenerator.Type != "" {
					produceBenchSpecData.TransactionGenerator = &scenarioConfig.ProducerOptions.TransactionGenerator
				}
				rawSpec, _ = json.Marshal(produceBenchSpecData)
			}

		case CONSUME_BENCH_SPEC_CLASS:
			{
				if scenarioConfig.TopicSpec.TopicName == "" {
					scenarioConfig.TopicSpec.TopicName = "TrogdorTopic[1-1]"
				}

				consumeBenchSpecData := consumerSpec{
					Class:                scenarioConfig.Class,
					StartMs:              scenarioConfig.StartMs,
					DurationMs:           scenarioConfig.LoopDurationMs,
					ConsumerNode:         clientNode,
					BootstrapServers:     scenarioConfig.BootstrapServers,
					TargetMessagesPerSec: messagesPerSec,
					MaxMessages:          maxMessages,
					CommonClientConf:     &scenarioConfig.AdminConf,
					ConsumerGroup:        scenarioConfig.ConsumerOptions.ConsumerGroup, // trogdor will generate a random group if given an empty one
					ActiveTopics:         []string{scenarioConfig.TopicSpec.TopicName},
				}
				rawSpec, _ = json.Marshal(consumeBenchSpecData)
			}
		}

		SpecData := TaskSpec{
			ID:   fmt.Sprintf("%s-loadLoop%d-clientNode%s", scenarioConfig.ScenarioID, loadLoop, clientNode),
			Spec: rawSpec,
		}
		scenario = append(scenario, SpecData)
	}

	return scenario
}

func (r *ScenarioSpec) CreateScenario(scenarioConfig ScenarioConfig) {
	var scenario []TaskSpec

	if scenarioConfig.NumberOfLoops > 0 {
		for loadLoop := 0; loadLoop < scenarioConfig.NumberOfLoops; loadLoop++ {
			scenario = r.createAgentTasks(scenarioConfig, scenario, loadLoop)
			// increment startMs for the next tasks
			scenarioConfig.StartMs = scenarioConfig.StartMs + scenarioConfig.LoopCoolDownMs + scenarioConfig.LoopDurationMs
		}
	} else { // single iteration
		scenario = r.createAgentTasks(scenarioConfig, scenario, 1)
	}

	r.TaskSpecs = scenario
}

// Returns all the Trogdor Tasks from the coordinator that match the filter
// Note that earliestStartMs and latestStartMs denote the time
// when the task was actually started, not scheduled to start
func TaskStatuses(coordinatorURL string, earliestStartMs int64, latestStartMs int64, state string) (map[string]*TaskStatus, error) {
	req, err := http.Get(fmt.Sprintf("%s/coordinator/tasks?firstStartMs=%d&lastStartMs=%d&state=%s",
		coordinatorURL, earliestStartMs, latestStartMs, state))
	if err != nil {
		return nil, err
	}
	if req.StatusCode > 299 {
		return nil, errors.New(fmt.Sprintf(
			`Request for the status of tasks (earliestStartMs: %s, latestStartMs: %s) returned status code %s`,
			earliestStartMs, latestStartMs, req.Status))
	}

	return parseStatuses(req.Body)
}

type taskStatuses struct {
	Tasks map[string]*TaskStatus `json:"tasks"`
}

// parses statuses returned from the /coordinator/tasks endpoint
func parseStatuses(reader io.Reader) (map[string]*TaskStatus, error) {
	var statuses taskStatuses
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&statuses)
	if err != nil {
		return nil, err
	}

	return statuses.Tasks, nil
}

func (r *TaskSpec) Status(coordinatorURL string) (*TaskStatus, error) {
	req, err := http.Get(fmt.Sprintf("%s/coordinator/tasks/%s", coordinatorURL, r.ID))
	if err != nil {
		return nil, err
	}
	if req.StatusCode > 299 {
		return nil, errors.New(fmt.Sprintf(`Request for the status of task "%s" returned status code %s`, r.ID, req.Status))
	}

	return r.parseStatus(req.Body)
}

func (r *TaskSpec) parseStatus(reader io.Reader) (*TaskStatus, error) {
	var taskStatus TaskStatus
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&taskStatus)
	if err != nil {
		return nil, err
	}

	return &taskStatus, err
}

func (r *TaskSpec) CreateTask(coordinatorURL string) (*http.Response, []byte, error) {

	out, err := json.Marshal(r)

	if err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/coordinator/task/create", coordinatorURL), bytes.NewBuffer(out))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	return resp, body, nil
}

func DeleteTask(coordinatorURL string, taskId string) (*http.Response, []byte, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/coordinator/tasks?taskId=%s", coordinatorURL, taskId), nil)
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

func (r *TaskStatus) isConsumeTask() bool {
	return r.Spec.Class == CONSUME_BENCH_SPEC_CLASS
}

func (r *TaskStatus) isProduceTask() bool {
	return r.Spec.Class == PRODUCE_BENCH_SPEC_CLASS
}

func (r *TaskStatus) produceTaskStatus() (*produceTaskStatus, error) {
	if !r.isProduceTask() {
		return nil, errors.New(fmt.Sprintf("the task is a %s, not a produce task", r.Spec.Class))
	}
	var taskStatus produceTaskStatus
	err := json.Unmarshal(r.Status, &taskStatus)
	if err != nil {
		return nil, err
	}

	return &taskStatus, err
}

func (r *TaskStatus) consumeTaskStatus() (map[string]*consumeTaskStatus, error) {
	if !r.isConsumeTask() {
		return nil, errors.New(fmt.Sprintf("the task is a %s, not a consume task", r.Spec.Class))
	}
	var taskStatuses map[string]*consumeTaskStatus
	err := json.Unmarshal(r.Status, &taskStatuses)
	if err != nil {
		return nil, err
	}

	return taskStatuses, err
}
