package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;


import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ConnectionSensorsTest {
  private TenantMetricsTestUtils utils;
  private ConnectionSensors sensors;
  private Metrics metrics;
  private final String activeAuthConnectionsCountMetricName = "active-authenticated-connection-count";
  private String[] connectionMetrics = new String[]{
    "successful-authentication-rate", "successful-authentication-total", activeAuthConnectionsCountMetricName
  };

  @Before
  public void setUp() {
    String tenantName = "tenant";
    String userName = "user";
    metrics = new Metrics();
    MultiTenantPrincipal principal = new MultiTenantPrincipal(userName, new TenantMetadata(tenantName, "cluster-1"));
    utils = new TenantMetricsTestUtils(metrics, tenantName, userName);
    sensors = new ConnectionSensorBuilder(metrics, principal).build();
  }

  @Test
  public void testRecordConnectionAndDisconnectionBringsActiveConnectionsToZero() {
    for (int i = 0; i < 5; i++) {
      sensors.recordAuthenticatedConnection();
      assertEquals((double) i + 1, (double) utils.metric(activeAuthConnectionsCountMetricName).metricValue(), 0);
    }
    for (int i = 5; i > 0; i--) {
      sensors.recordAuthenticatedDisconnection();
      assertEquals((double) i - 1, (double) utils.metric(activeAuthConnectionsCountMetricName).metricValue(), 0);
    }

    Map<String, KafkaMetric> metrics = utils.verifyTenantMetrics("successful-authentication-rate", "successful-authentication-total");
    assertEquals(5.0, (double) metrics.get("successful-authentication-total").metricValue(), 0);
    assertEquals(0.0, (double) utils.metric(activeAuthConnectionsCountMetricName).metricValue(), 0);
  }

  @Test
  public void testRecordAuthenticatedConnectionIncrementsRateTotalAndActiveConnections() {
    sensors.recordAuthenticatedConnection();
    utils.verifyTenantMetrics("successful-authentication-rate", "successful-authentication-total", activeAuthConnectionsCountMetricName);
  }

  /**
   * Assert that different tenants/users use separate metrics (different tags)
   */
  @Test
  public void testRecordAuthenticatedConnectionsPerTenant() {
    TenantMetadata firstTenantMetadata = new TenantMetadata("tenant1", "cluster-1");
    MultiTenantPrincipal firstTenantU1 = new MultiTenantPrincipal("tenant1_user1", firstTenantMetadata);
    MultiTenantPrincipal firstTenantU2 = new MultiTenantPrincipal("tenant1_user2", firstTenantMetadata);
    MultiTenantPrincipal secondTenantU1 = new MultiTenantPrincipal("tenant2_user1", new TenantMetadata("tenant2", "cluster-1"));

    ConnectionSensors firstTenantU1Sensors = new ConnectionSensorBuilder(metrics, firstTenantU1).build();
    ConnectionSensors firstTenantU2Sensors = new ConnectionSensorBuilder(metrics, firstTenantU2).build();
    ConnectionSensors secondTenantU1Sensors = new ConnectionSensorBuilder(metrics, secondTenantU1).build();

    // increment connections for tenant1-user1, all others should be unaffected
    for (int i = 0; i < 5; i++) {
      firstTenantU1Sensors.recordAuthenticatedConnection();
    }
    TenantMetricsTestUtils.verifyTenantMetrics(metrics, "tenant1", "tenant1_user1", connectionMetrics);
    TenantMetricsTestUtils.verifyTenantMetrics(metrics, "tenant1", "tenant1_user2", true, connectionMetrics);
    TenantMetricsTestUtils.verifyTenantMetrics(metrics, "tenant2", "tenant2_user1", true, connectionMetrics);

    // increment connections for tenant1_user2, all others should be unaffected
    firstTenantU2Sensors.recordAuthenticatedConnection();
    Map<String, KafkaMetric> firstTenantU1Metrics = TenantMetricsTestUtils.verifyTenantMetrics(
            metrics, "tenant1", "tenant1_user1", connectionMetrics);
    Map<String, KafkaMetric> firstTenantU2Metrics = TenantMetricsTestUtils.verifyTenantMetrics(
            metrics, "tenant1", "tenant1_user2", connectionMetrics);
    assertEquals(5.00,
            (double) firstTenantU1Metrics.get(activeAuthConnectionsCountMetricName).metricValue(), 0);  // should be unaffected
    assertEquals(1.00,
            (double) firstTenantU2Metrics.get(activeAuthConnectionsCountMetricName).metricValue(), 0);
    TenantMetricsTestUtils.verifyTenantMetrics(metrics, "tenant2", "tenant2_user1", true, connectionMetrics);

    // increment connections for tenant2_user1, all others should be unaffected
    for (int i = 0; i < 3; i++) {
      secondTenantU1Sensors.recordAuthenticatedConnection();
    }
    Map<String, KafkaMetric> secondTenantU1Metrics = TenantMetricsTestUtils.verifyTenantMetrics(
            metrics, "tenant2", "tenant2_user1", connectionMetrics);
    assertEquals(3.00,
            (double) secondTenantU1Metrics.get(activeAuthConnectionsCountMetricName).metricValue(), 0);
    // tenant1 metrics should be unaffected
    firstTenantU1Metrics = TenantMetricsTestUtils.verifyTenantMetrics(
            metrics, "tenant1", "tenant1_user1", connectionMetrics);
    firstTenantU2Metrics = TenantMetricsTestUtils.verifyTenantMetrics(
            metrics, "tenant1", "tenant1_user2", connectionMetrics);
    assertEquals(5.00,
            (double) firstTenantU1Metrics.get(activeAuthConnectionsCountMetricName).metricValue(), 0);
    assertEquals(1.00,
            (double) firstTenantU2Metrics.get(activeAuthConnectionsCountMetricName).metricValue(), 0);
  }
}
