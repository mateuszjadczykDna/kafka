// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import java.lang.reflect.Field;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TenantQuotaCallbackTest {

  private static final double EPS = 0.0001;

  private final int brokerId = 1;
  private TestCluster testCluster;
  private TenantQuotaCallback quotaCallback;

  @Before
  public void setUp() {
    TenantQuotaCallback.closeAll();
    quotaCallback = new TenantQuotaCallback();
    Map<String, Object> configs = quotaCallbackProps();
    quotaCallback.configure(configs);
    Map<String, QuotaConfig> tenantQuotas = new HashMap<>();
    tenantQuotas.put("tenant1", quotaConfig(1000, 2000, 300));
    tenantQuotas.put("tenant2", quotaConfig(2000, 3000, 400));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
  }

  @After
  public void tearDown() {
    quotaCallback.close();
  }

  @Test
  public void testTenantQuota() {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // Partitions divided between two brokers
    setPartitionLeaders("tenant1_topic1", 0, 5, brokerId);
    setPartitionLeaders("tenant1_topic2", 0, 5, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 150);

    // Other tenant's partitions shouldn't impact quota
    setPartitionLeaders("tenant2_topic1", 0, 5, brokerId);
    setPartitionLeaders("tenant2_topic2", 0, 5, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 150);

    // Delete topic
    deleteTopic("tenant1_topic2");
    verifyQuotas(principal, 1000, 2000, 300);

    // Add another topic
    setPartitionLeaders("tenant1_topic3", 0, 5, brokerId + 2);
    verifyQuotas(principal, 500, 1000, 150);

    // Change partition leader
    setPartitionLeaders("tenant1_topic3", 1, 1, brokerId);
    verifyQuotas(principal, 600, 1200, 180);
  }

  @Test
  public void testSmallNumberOfPartitionsNoMinPartitions() throws Exception {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // One partition on one broker
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    verifyQuotas(principal, 1000, 2000, 300);

    // Two partitions on one broker
    setPartitionLeaders("tenant1_topic2", 0, 1, brokerId);
    verifyQuotas(principal, 1000, 2000, 300);

    // Add two more partitions on another broker
    setPartitionLeaders("tenant1_topic3", 0, 2, brokerId + 1);
    verifyQuotas(principal, 500, 1000, 150);

    // Add six more partitions on another broker
    setPartitionLeaders("tenant1_topic4", 0, 6, brokerId + 2);
    verifyQuotas(principal, 200, 400, 60);
  }

  @Test
  public void testSmallNumberOfPartitions() throws Exception {
    createCluster(10);
    configureMinPartitions(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // One partition on one broker
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    verifyQuotas(principal, 200, 400, 60);

    // Two partitions on one broker
    setPartitionLeaders("tenant1_topic2", 0, 1, brokerId);
    verifyQuotas(principal, 400, 800, 120);

    // Add two more partitions on another broker
    setPartitionLeaders("tenant1_topic3", 0, 2, brokerId + 1);
    verifyQuotas(principal, 400, 800, 120);

    // Add six more partitions on another broker
    setPartitionLeaders("tenant1_topic4", 0, 6, brokerId + 2);
    verifyQuotas(principal, 200, 400, 60);
  }

  /**
   * If there are no partitions for a tenant, we divide tenant quotas equally
   * amongst available brokers. This ensures that if a request was received before
   * metadata was refreshed on the quota callback, the client is not throttled for a long time.
   */
  @Test
  public void testNoPartitions() throws Exception {
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    verifyQuotas(principal, 200, 400, 60);

    setPartitionLeaders("tenant1_topic1", 0, 2, brokerId);
    verifyQuotas(principal, 1000, 2000, 300);

    configureMinPartitions(5);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
    verifyQuotas(principal, 400, 800, 120);

    // Delete all partitions
    createCluster(5);
    verifyQuotas(principal, 200, 400, 60);
  }

  /**
   * If no cluster metadata is available (there could be a small timing window where
   * client requests are processed before metadata is available), full tenant quota
   * is allocated to broker to avoid unnecessary throttling.
   */
  @Test
  public void testNoClusterMetadata() {
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));
    verifyQuotas(principal, 1000, 2000, 300);
  }

  /**
   * Tenants may be created with unlimited quota (e.g. if we dont want to enable request quotas)
   */
  @Test
  public void testUnlimitedTenantQuota() {
    createCluster(5);
    Map<String, QuotaConfig> tenantQuotas = new HashMap<>();
    tenantQuotas.put("tenant1", quotaConfig(Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));
    verifyUnlimitedQuotas(principal);

    setPartitionLeaders("tenant1_topic1", 0, 5, brokerId);
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
    setPartitionLeaders("tenant1_topic2", 0, 5, brokerId + 1);
    verifyUnlimitedQuotas(principal);

    tenantQuotas.put("tenant1", quotaConfig(1000, 2000, Integer.MAX_VALUE));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
    verifyQuota(ClientQuotaType.PRODUCE, principal, 500, "tenant1");
    verifyQuota(ClientQuotaType.FETCH, principal, 1000, "tenant1");
    verifyQuota(ClientQuotaType.REQUEST, principal, Integer.MAX_VALUE, null);
  }

  /**
   * Tenant quotas are refreshed periodically with a default 30 second interval.
   * We can apply a configurable default quota for tenants whose quota is not known
   * to avoid tenants overloading the broker during this period.
   */
  @Test
  public void testDefaultTenantQuota() {
    // By default, we don't apply quotas until tenant is known (i.e unlimited quota by default)
    createCluster(5);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
            new TenantMetadata("tenant100", "tenant100_cluster_id", false));
    verifyUnlimitedQuotas(principal);

    // We can apply configurable defaults using custom broker configs
    QuotaConfig defaultQuota = quotaConfig(1000L, 2000L, 10.0);
    TenantQuotaCallback.updateQuotas(Collections.emptyMap(), defaultQuota);
    MultiTenantPrincipal principal2 = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant101", "tenant101_cluster_id", false));
    verifyQuotas(principal2, 200, 400, 2);

    // Default quota updates should also apply to tenants currently using the default
    verifyQuotas(principal, 200, 400, 2);

    // Change default quota and verify again
    QuotaConfig newDefaultQuota = quotaConfig(3000L, 6000L, 30.0);
    TenantQuotaCallback.updateQuotas(Collections.emptyMap(), newDefaultQuota);
    verifyQuotas(principal2, 600, 1200, 6);

    verifyQuotas(principal, 600, 1200, 6);
  }

  /**
   * Quotas are not applied to non-tenant principals like broker principal.
   */
  @Test
  public void testNonTenantPrincipal() {
    createCluster(5);
    KafkaPrincipal principal = KafkaPrincipal.ANONYMOUS;
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
  }

  @Test
  public void testUnavailableBrokers() {
    createCluster(2);
    MultiTenantPrincipal principal = new MultiTenantPrincipal("userA",
        new TenantMetadata("tenant1", "tenant1_cluster_id", false));

    // Replicas may refer to nodes that are not available. Quotas should
    // still be calculated correctly.
    setPartitionLeaders("tenant1_topic1", 0, 1, brokerId);
    setPartitionLeaders("tenant1_topic2", 0, 1, 30);
    assertNull("Unavailable node created", testCluster.cluster().nodeById(30));
    assertNull("Unavailable node created",
        testCluster.cluster().partitionsForTopic("tenant1_topic2").get(0).leader());
    quotaCallback.updateClusterMetadata(testCluster.cluster());
    verifyQuotas(principal, 500, 1000, 150);
  }

  private Map<String, Object> quotaCallbackProps() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.id", String.valueOf(brokerId));
    configs.put(TenantQuotaCallback.MIN_PARTITIONS_CONFIG, "1");
    return configs;
  }

  private void createCluster(int numNodes) {
    testCluster = new TestCluster();
    for (int i = 0; i < numNodes; i++) {
      testCluster.addNode(i, "rack0");
    }
    Cluster cluster = testCluster.cluster();
    quotaCallback.updateClusterMetadata(cluster);
    assertEquals(cluster, quotaCallback.cluster());
  }

  private void verifyQuotas(MultiTenantPrincipal principal, double produceQuota, double fetchQuota, double requestQuota) {
    String tenant = principal.tenantMetadata().tenantName;
    verifyQuota(ClientQuotaType.PRODUCE, principal, produceQuota, tenant);
    verifyQuota(ClientQuotaType.FETCH, principal, fetchQuota, tenant);
    verifyQuota(ClientQuotaType.REQUEST, principal, requestQuota, tenant);
  }

  private void verifyQuota(ClientQuotaType type, KafkaPrincipal principal,
                           double expectedValue, String expectedTenantTag) {
    Map<String, String> metricTags = quotaCallback.quotaMetricTags(type, principal, "some-client");
    if (expectedTenantTag != null) {
      assertEquals(Collections.singletonMap("tenant", expectedTenantTag), metricTags);
    } else {
      assertTrue("Unexpected tags " + metricTags, metricTags.isEmpty());
    }
    Double quotaLimit = quotaCallback.quotaLimit(type, metricTags);
    assertEquals("Unexpected quota of type " + type, expectedValue, quotaLimit, EPS);
  }

  private void verifyUnlimitedQuotas(KafkaPrincipal principal) {
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      verifyQuota(quotaType, principal, QuotaConfig.UNLIMITED_QUOTA.quota(quotaType), null);
    }
  }

  private void setPartitionLeaders(String topic, int firstPartition, int count,
                                   Integer leaderBrokerId) {
    testCluster.setPartitionLeaders(topic, firstPartition, count, leaderBrokerId);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
  }

  private void deleteTopic(String topic) {
    testCluster.deleteTopic(topic);
    quotaCallback.updateClusterMetadata(testCluster.cluster());
  }

  private QuotaConfig quotaConfig(long producerByteRate, long consumerByteRate, double requestPercentage) {
    return new QuotaConfig(producerByteRate, consumerByteRate, requestPercentage, QuotaConfig.UNLIMITED_QUOTA);
  }

  private void configureMinPartitions(int minPartitions) throws Exception {
    Field field = quotaCallback.getClass().getDeclaredField("minPartitionsForMaxQuota");
    field.setAccessible(true);
    field.set(quotaCallback, minPartitions);
  }
}