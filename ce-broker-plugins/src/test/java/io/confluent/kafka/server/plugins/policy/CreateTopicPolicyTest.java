// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import com.google.common.collect.ImmutableMap;

import java.util.Optional;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateTopicPolicyTest {

  private static final String CLUSTER_ID = "mockClusterId";
  private static final String TENANT_PREFIX = "xx_";

  private static final String TOPIC = "xx_test-topic";
  private static final short REPLICATION_FACTOR = 5;
  private static final short MIN_IN_SYNC_REPLICAS = 4;
  private static final int MAX_PARTITIONS = 21;
  private static final int MAX_MESSAGE_BYTES = 4242;

  private CreateTopicPolicy policy;
  private RequestMetadata requestMetadata;

  @Before
  public void setUp() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, String.valueOf(REPLICATION_FACTOR));
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(MIN_IN_SYNC_REPLICAS));
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, String.valueOf(MAX_PARTITIONS));
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "INTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");
    policy = new CreateTopicPolicy();
    policy.configure(config);

    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(MAX_MESSAGE_BYTES))
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(MIN_IN_SYNC_REPLICAS))
        .build();
    requestMetadata = mock(RequestMetadata.class);
    when(requestMetadata.topic()).thenReturn(TOPIC);
    when(requestMetadata.replicationFactor()).thenReturn(REPLICATION_FACTOR);
    when(requestMetadata.numPartitions()).thenReturn(MAX_PARTITIONS);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
  }

  @Test
  public void testValidateOk() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions - 1;
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      prepareForOneValidateCall(clientEnv, ImmutableMap.of(TOPIC, currentPartitions));
      policy.ensureValidPartitionCount(clientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
    }
  }

  @Test
  public void acceptsExactlyMaxPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions;
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      prepareForOneValidateCall(clientEnv, ImmutableMap.of(TOPIC, currentPartitions));
      policy.ensureValidPartitionCount(clientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
    }
  }

  @Test
  public void testValidateDoesNotCountOtherTopicPartitions() {
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      prepareForOneValidateCall(clientEnv, ImmutableMap.of(TOPIC, MAX_PARTITIONS));
      policy.ensureValidPartitionCount(clientEnv.adminClient(), "badprefix_", MAX_PARTITIONS);
    }
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsRequestOverMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions + 1;
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      prepareForOneValidateCall(clientEnv, ImmutableMap.of(TOPIC, currentPartitions));
      policy.ensureValidPartitionCount(clientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
    }
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsCurrentExceedMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      prepareForOneValidateCall(clientEnv, ImmutableMap.of(TOPIC, currentPartitions));
      policy.ensureValidPartitionCount(clientEnv.adminClient(), TENANT_PREFIX, MAX_PARTITIONS + 1);
    }
  }

  @Test(expected = RuntimeException.class)
  public void rejectsWhenNoResponse() {
    try (AdminClientUnitTestEnv clientEnv = getAdminClientEnv()) {
      policy.ensureValidPartitionCount(clientEnv.adminClient(), TENANT_PREFIX, 1);
    }
  }

  // will throw exception because of failure to use AdminClient without kafka cluster
  @Test(expected = RuntimeException.class)
  public void validateParamsSetOk() throws Exception {
    policy.validate(requestMetadata);
  }

  // will throw exception because of failure to use AdminClient without kafka cluster
  @Test(expected = RuntimeException.class)
  public void validateNoReplicationNoTopicConfigGivenOk() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .build();
    when(requestMetadata.replicationFactor()).thenReturn(null);
    when(requestMetadata.numPartitions()).thenReturn(10);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  // will throw exception because of failure to use AdminClient without kafka cluster
  @Test(expected = RuntimeException.class)
  public void validateValidTopicConfigsOk() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100")
        .put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "100")
        .put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime")
        .put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100")
        .put(TopicConfig.RETENTION_BYTES_CONFIG, "100")
        .put(TopicConfig.RETENTION_MS_CONFIG, "135217728")
        .put(TopicConfig.SEGMENT_MS_CONFIG, "600")
        .build();
    when(requestMetadata.replicationFactor()).thenReturn(null);
    when(requestMetadata.numPartitions()).thenReturn(10);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void validateInvalidTopicConfigsNotOk() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100") // allowed
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "5")  // disallowed
        .put(TopicConfig.RETENTION_MS_CONFIG, "135217728")  // allowed
        .build();
    when(requestMetadata.replicationFactor()).thenReturn(null);
    when(requestMetadata.numPartitions()).thenReturn(10);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsNoPartitionCountGiven() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .build();
    when(requestMetadata.replicationFactor()).thenReturn(null);
    when(requestMetadata.numPartitions()).thenReturn(null);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBadRepFactor() throws Exception {
    when(requestMetadata.replicationFactor()).thenReturn((short) 6);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBadMinIsrs() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = RuntimeException.class)
  public void rejectsBadNumPartitions() throws Exception {
    when(requestMetadata.numPartitions()).thenReturn(22);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectDeleteRetentionMsTooHigh() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "60566400001")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooLow() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.SEGMENT_BYTES_CONFIG, "" + (50 * 1024 * 1024 - 1))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooHigh() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741825")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentMsTooLow() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
            .put(TopicConfig.SEGMENT_MS_CONFIG, "" + (500 * 1000))
            .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void validateGetBootstrapBrokerFromConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    String bootstrapBroker = policy.getBootstrapBrokerForListener("INTERNAL", config);
    assertNotNull(bootstrapBroker);
    assertEquals(bootstrapBroker, "broker-1:9071");
  }

  @Test(expected = ConfigException.class)
  public void testNoListenersFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("listener.security.protocol.map",
               "INTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test(expected = ConfigException.class)
  public void testNoInternalListenerFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "INTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyInternalListenerFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "INTERNAL://,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "INTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test
  public void validateGetSecurityProtocolFromConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("listener.security.protocol.map",
               "INTERNAL:PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");
    String securityProtocol = policy.getListenerSecurityProtocol("INTERNAL", config);
    assertNotNull(securityProtocol);
    assertEquals(securityProtocol, "PLAINTEXT");
  }

  @Test(expected = ConfigException.class)
  public void testNoSecurityProtocolMapFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test(expected = ConfigException.class)
  public void testNoInternalSecurityProtocolFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyInternalSecurityProtocolFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "INTERNAL:,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  @Test(expected = ConfigException.class)
  public void testNonPlaintextInternalSecurityProtocolFailsConfigure() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put("advertised.listeners",
               "INTERNAL://broker-1:9071,REPLICATION://broker-1:9072,EXTERNAL://broker-1");
    config.put("listener.security.protocol.map",
               "INTERNAL:SASL_PLAINTEXT,REPLICATION:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT");

    CreateTopicPolicy topicPolicy = new CreateTopicPolicy();
    topicPolicy.configure(config);
  }

  private static AdminClientUnitTestEnv getAdminClientEnv(int numBrokers,
                                                          Set<String> internalTopics) {
    HashMap<Integer, Node> nodes = new HashMap<>();
    for (int i = 0; i < numBrokers; i++) {
      nodes.put(i, new Node(i, "localhost", 8121 + i));
    }
    Cluster cluster = new Cluster(CLUSTER_ID, nodes.values(),
                                  Collections.<PartitionInfo>emptySet(), internalTopics,
                                  Collections.<String>emptySet(), nodes.get(0));

    AdminClientUnitTestEnv clientEnv =
        new AdminClientUnitTestEnv(cluster, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10");

    clientEnv.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

    return clientEnv;
  }

  private static AdminClientUnitTestEnv getAdminClientEnv() {
    return getAdminClientEnv(3, Collections.<String>emptySet());
  }

  private static void prepareForOneValidateCall(AdminClientUnitTestEnv clientEnv,
                                                Set<String> internalTopics,
                                                Map<String, Integer> topicPartitions) {
    List<MetadataResponse.TopicMetadata> topicMetadataList = new ArrayList<>();
    for (Map.Entry<String, Integer> topicPartition: topicPartitions.entrySet()) {
      topicMetadataList.add(
          new MetadataResponse.TopicMetadata(Errors.NONE, topicPartition.getKey(),
                                             internalTopics.contains(topicPartition.getKey()),
                                             partitionMetadatas(clientEnv, topicPartition.getValue()))
      );
    }

    // each CreateTopicPolicy.ensureValidPartitionCount calls 3 admin client methods which expect
    // a response
    clientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(clientEnv.cluster().nodes(), CLUSTER_ID, clientEnv.cluster().controller().id(), topicMetadataList));
    clientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(clientEnv.cluster().nodes(), CLUSTER_ID, clientEnv.cluster().controller().id(), topicMetadataList));
    clientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(clientEnv.cluster().nodes(), CLUSTER_ID, clientEnv.cluster().controller().id(), topicMetadataList));
  }

  private static void prepareForOneValidateCall(AdminClientUnitTestEnv clientEnv,
                                                Map<String, Integer> topicPartitions) {
    prepareForOneValidateCall(clientEnv, Collections.<String>emptySet(), topicPartitions);
  }

  private static List<MetadataResponse.PartitionMetadata> partitionMetadatas(AdminClientUnitTestEnv clientEnv, int numPartitions) {
    List<MetadataResponse.PartitionMetadata> metadatas = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      metadatas.add(new MetadataResponse.PartitionMetadata(Errors.NONE,
                                                           i,
                                                           clientEnv.cluster().nodes().get(0),
                                                           Optional.empty(),
                                                           clientEnv.cluster().nodes(),
                                                           clientEnv.cluster().nodes(),
                                                           clientEnv.cluster().nodes()));
    }
    return metadatas;
  }

}
