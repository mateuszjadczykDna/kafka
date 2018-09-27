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
  private static final String TOPIC = "xx_test-topic";
  private static final String TENANT_PREFIX= "xx_";
  private static final short REPLICATION_FACTOR = 5;
  private static final int MAX_PARTITIONS = 21;
  private static final short MIN_IN_SYNC_REPLICAS = 4;

  private CreateTopicPolicy policy;
  private RequestMetadata requestMetadata;
  private Map<String, String> config = new HashMap<>();

  @Before
  public void setUp() throws Exception {
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
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    prepareForOneValidateCall(mockClientEnv, ImmutableMap.of(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
  }

  @Test
  public void acceptsExactlyMaxPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions;
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    prepareForOneValidateCall(mockClientEnv, ImmutableMap.of(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
  }

  @Test
  public void testValidateDoesNotCountOtherTopicPartitions() {
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    prepareForOneValidateCall(mockClientEnv, ImmutableMap.of(TOPIC, MAX_PARTITIONS));
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), "badprefix_", MAX_PARTITIONS);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsRequestOverMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions + 1;
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    prepareForOneValidateCall(mockClientEnv, ImmutableMap.of(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), TENANT_PREFIX, requestedPartitions);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsCurrentExceedMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    prepareForOneValidateCall(mockClientEnv, ImmutableMap.of(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), TENANT_PREFIX, MAX_PARTITIONS + 1);
  }

  @Test(expected = RuntimeException.class)
  public void rejectsWhenNoResponse() {
    AdminClientUnitTestEnv mockClientEnv = getMockClientEnv();
    policy.ensureValidPartitionCount(mockClientEnv.adminClient(), TENANT_PREFIX, 1);
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
    when(requestMetadata.replicationFactor()).thenReturn((short)6);
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

  private static AdminClientUnitTestEnv getMockClientEnv(int numBrokers,
                                                          Set<String> internalTopics) {
    HashMap<Integer, Node> nodes = new HashMap<>();
    for (int i = 0; i < numBrokers; i++) {
      nodes.put(i, new Node(i, "localhost", 8121 + i));
    }
    Cluster cluster = new Cluster(CLUSTER_ID, nodes.values(),
                                  Collections.<PartitionInfo>emptySet(), internalTopics,
                                  Collections.<String>emptySet(), nodes.get(0));

    AdminClientUnitTestEnv mockClientEnv =
        new AdminClientUnitTestEnv(cluster, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10");

    mockClientEnv.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
    mockClientEnv.kafkaClient().prepareMetadataUpdate(cluster, Collections.<String>emptySet());
    mockClientEnv.kafkaClient().setNode(cluster.controller());

    return mockClientEnv;
  }

  private static AdminClientUnitTestEnv getMockClientEnv() {
    return getMockClientEnv(3, Collections.<String>emptySet());
  }

  private static void prepareForOneValidateCall(AdminClientUnitTestEnv mockClientEnv,
                                                Set<String> internalTopics,
                                                Map<String, Integer> topicPartitions) {
    List<MetadataResponse.TopicMetadata> topicMetadataList = new ArrayList<>();
    for (Map.Entry<String, Integer> topicPartition: topicPartitions.entrySet()) {
      topicMetadataList.add(
          new MetadataResponse.TopicMetadata(Errors.NONE, topicPartition.getKey(),
                                             internalTopics.contains(topicPartition.getKey()),
                                             partitionMetadatas(mockClientEnv, topicPartition.getValue()))
      );
    }

    // each CreateTopicPolicy.ensureValidPartitionCount calls 3 admin client methods which expect
    // a response
    mockClientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(mockClientEnv.cluster().nodes(), CLUSTER_ID, mockClientEnv.cluster().controller().id(), topicMetadataList));
    mockClientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(mockClientEnv.cluster().nodes(), CLUSTER_ID, mockClientEnv.cluster().controller().id(), topicMetadataList));
    mockClientEnv.kafkaClient().prepareResponse(
        new MetadataResponse(mockClientEnv.cluster().nodes(), CLUSTER_ID, mockClientEnv.cluster().controller().id(), topicMetadataList));
  }

  private static void prepareForOneValidateCall(AdminClientUnitTestEnv mockClientEnv,
                                                Map<String, Integer> topicPartitions) {
    prepareForOneValidateCall(mockClientEnv, Collections.<String>emptySet(), topicPartitions);
  }

  private static List<MetadataResponse.PartitionMetadata> partitionMetadatas(AdminClientUnitTestEnv mockClientEnv, int numPartitions) {
    List<MetadataResponse.PartitionMetadata> metadatas = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      metadatas.add(new MetadataResponse.PartitionMetadata(Errors.NONE,
                                                           i,
                                                           mockClientEnv.cluster().nodes().get(0),
                                                           Optional.empty(),
                                                           mockClientEnv.cluster().nodes(),
                                                           mockClientEnv.cluster().nodes(),
                                                           mockClientEnv.cluster().nodes()));
    }
    return metadatas;
  }

}
