// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.metrics.PartitionSensors.PercentilesSensorCreator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class PartitionSensorBuilder extends AbstractSensorBuilder<PartitionSensors> {
  static final String BYTES_IN = "partition-bytes-in";
  static final String BYTES_OUT = "partition-bytes-out";
  static final String BROKER_SENSOR_PREFIX = "broker-";

  private final Map<String, PercentilesSensorCreator> partitionSensorCreators;

  public PartitionSensorBuilder(Metrics metrics, MultiTenantPrincipal principal) {
    super(metrics, principal);
    String tenant = principal.tenantMetadata().tenantName;

    partitionSensorCreators = new HashMap<>(4);
    partitionSensorCreators.put(BYTES_IN,
        new PercentilesSensorCreator(Optional.of(tenant), BYTES_IN));
    partitionSensorCreators.put(BYTES_OUT,
        new PercentilesSensorCreator(Optional.of(tenant), BYTES_OUT));
    partitionSensorCreators.put(BROKER_SENSOR_PREFIX + BYTES_IN,
        new PercentilesSensorCreator(Optional.empty(), BYTES_IN));
    partitionSensorCreators.put(BROKER_SENSOR_PREFIX + BYTES_OUT,
        new PercentilesSensorCreator(Optional.empty(), BYTES_OUT));
  }

  @Override
  public PartitionSensors build() {
    Map<String, Sensor> sensors = getOrCreateSuffixedSensors();

    return new PartitionSensors(principal, sensors, this);
  }

  @Override
  <T> Sensor createSensor(Map<T, ? extends AbstractSensorCreator> sensorCreators,
                                   T sensorKey, String sensorName) {
    return sensorCreators.get(sensorKey).createSensor(metrics, sensorName);
  }

  @Override
  protected String sensorSuffix(String name, MultiTenantPrincipal principal) {
    if (name.startsWith(BROKER_SENSOR_PREFIX)) {
      return ":";
    } else {
      return String.format(":%s-%s",
          TenantMetrics.TENANT_TAG, principal.tenantMetadata().tenantName);
    }
  }

  @Override
  protected Map<String, PercentilesSensorCreator> sensorCreators() {
    return partitionSensorCreators;
  }
}
