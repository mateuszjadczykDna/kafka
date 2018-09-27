// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

public class TenantMetrics {

  private EnumMap<ApiKeys, ApiSensors> apiSensors = new EnumMap<>(ApiKeys.class);

  public void recordRequest(Metrics metrics, MultiTenantPrincipal principal, ApiKeys apiKey,
      long requestSize) {
    ApiSensors sensors = apiSensors(metrics, principal, apiKey);
    sensors.recordRequest(requestSize);
  }

  public void recordResponse(Metrics metrics, MultiTenantPrincipal principal, ApiKeys apiKey,
      long responseSize, long responseTimeNanos, Map<Errors, Integer> errorCounts) {

    ApiSensors sensors = apiSensors(metrics, principal, apiKey);
    Set<Errors> newErrors = sensors.errorsWithoutSensors(errorCounts.keySet());
    if (!newErrors.isEmpty()) {
      ApiSensorBuilder builder = new ApiSensorBuilder(metrics, principal, apiKey);
      builder.addErrorSensors(sensors, newErrors);
    }

    sensors.recordResponse(responseSize, responseTimeNanos);
    sensors.recordErrors(errorCounts);
  }

  private ApiSensors apiSensors(Metrics metrics, MultiTenantPrincipal principal, ApiKeys apiKey) {
    ApiSensors sensors = apiSensors.get(apiKey);
    if (sensors == null) {
      sensors =  new ApiSensorBuilder(metrics, principal, apiKey).build();
      apiSensors.put(apiKey, sensors);
    }
    return sensors;
  }
}
