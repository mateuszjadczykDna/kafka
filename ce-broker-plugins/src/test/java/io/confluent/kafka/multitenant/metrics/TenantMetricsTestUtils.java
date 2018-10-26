package io.confluent.kafka.multitenant.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TenantMetricsTestUtils {
  private Metrics metrics;
  private final String tenantName;
  private final String userName;

  public TenantMetricsTestUtils(Metrics metrics, String tenantName, String userName) {
    this.metrics = metrics;
    this.tenantName = tenantName;
    this.userName = userName;
  }

  public Map<String, KafkaMetric> verifyTenantMetrics(String... expectedMetrics) {
    return verifyTenantMetrics(metrics, tenantName, userName, expectedMetrics);
  }

  /**
   * Given a list of metric names, this method verifies that:
   *  every metric exists, has a tenant tag, has some non-default value
   *  and that Sensors associated with the metrics exist.
   *
   * @return A map of KafkaMetric instances accessible by their name. e.g { "produced-bytes": KafkaMetric(...) }.
   *         Only contains the metrics in the expectedMetrics argument
   */
  public static Map<String, KafkaMetric> verifyTenantMetrics(Metrics metrics, String tenantName, String userName, String... expectedMetrics) {
    return verifyTenantMetrics(metrics, tenantName, userName, false, expectedMetrics);
  }
  public static Map<String, KafkaMetric> verifyTenantMetrics(Metrics metrics, String tenantName,
                                                             String userName, boolean assertEmpty, String... expectedMetrics) {
    Set<String> tenantMetrics = new HashSet<>();
    Map<String, KafkaMetric> metricsByName = new HashMap<>();
    List<String> expectedMetricsList = Arrays.asList(expectedMetrics);
    for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
      MetricName metricName = entry.getKey();
      String tenant = metricName.tags().get(TenantMetrics.TENANT_TAG);
      String user = metricName.tags().get(TenantMetrics.USER_TAG);
      if (!expectedMetricsList.contains(metricName.name()) || !tenantName.equals(tenant) || !userName.equals(user)) {
        continue;
      }

      KafkaMetric metric = entry.getValue();
      metricsByName.put(metricName.name(), metric);
      tenantMetrics.add(metricName.name());
      assertEquals(tenantName, tenant);
      assertEquals(userName, user);
      double value = (Double) metric.metricValue();
      if (assertEmpty) {
        assertEquals(String.format("Metric (%s) was recorded: %s", metricName.name(), value), value, 0.0, 0);
      } else {
        assertTrue(String.format("Metric (%s) not recorded: %s", metricName.name(), value), value > 0.0);
      }
    }
    verifySensors(metrics, tenantName, userName, expectedMetrics);
    return metricsByName;
  }

  public KafkaMetric metric(String wantedMetricName) {
    return metric(metrics, wantedMetricName);
  }

  public KafkaMetric metric(Metrics metrics, String wantedMetricName) {
    for (Map.Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
      MetricName metricName = entry.getKey();
      if (wantedMetricName.equals(metricName.name())) {
        return entry.getValue();
      }
    }
    return null;
  }


  public static void verifySensors(Metrics metrics, String tenantName, String userName, String... expectedMetrics) {
    for (String metricName : expectedMetrics) {
      String name = metricName.substring(0, metricName.lastIndexOf('-')); // remove -rate/-total/-count
      String sensorName = String.format("%s:%s-%s:%s-%s", name, TenantMetrics.TENANT_TAG,
              tenantName, TenantMetrics.USER_TAG, userName);
      Sensor sensor = metrics.getSensor(sensorName);
      assertNotNull("Sensor not found " + sensorName, sensor);
    }
  }
}
