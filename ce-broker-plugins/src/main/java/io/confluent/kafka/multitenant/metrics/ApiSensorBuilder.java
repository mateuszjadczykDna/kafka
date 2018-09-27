// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

public class ApiSensorBuilder {

  public static final long EXPIRY_SECONDS = TimeUnit.DAYS.toSeconds(7);
  private static final String GROUP = "tenant-metrics";
  private static final String REQUEST_TAG = "request";
  private static final String ERROR_TAG = "error";
  private static final String TENANT_TAG = "tenant";
  private static final String USER_TAG = "user";

  private static final String REQUEST_RATE = "request";
  private static final String REQUEST_BYTE_RATE = "request-byte";
  private static final String RESPONSE_BYTE_RATE = "response-byte";
  private static final String RESPONSE_TIME_NANOS = "response-time-ns";
  private static final String ERROR_COUNT = "error";
  private static final Map<String, AbstractSensorCreator> requestResponseSensorCreators;
  private static final EnumMap<Errors, AbstractSensorCreator> errorSensorCreators;

  private static final ReadWriteLock lock = new ReentrantReadWriteLock();

  static {
    requestResponseSensorCreators = new HashMap<>();
    requestResponseSensorCreators.put(REQUEST_RATE,
        new RequestMeterSensorCreator(REQUEST_RATE, "requests"));
    requestResponseSensorCreators.put(REQUEST_BYTE_RATE,
        new RequestSensorCreator(REQUEST_BYTE_RATE, "request bytes", true, true));
    requestResponseSensorCreators.put(RESPONSE_BYTE_RATE,
        new RequestSensorCreator(RESPONSE_BYTE_RATE, "response bytes", true, true));
    requestResponseSensorCreators.put(RESPONSE_TIME_NANOS,
        new RequestMinMaxAvgSensorCreator(RESPONSE_TIME_NANOS, "request processing time in nanos"));
    errorSensorCreators = new EnumMap<>(Errors.class);
    for (Errors error : Errors.values()) {
      errorSensorCreators.put(error, new ErrorCountSensorCreator(error));
    }
  }

  private final Metrics metrics;
  private final MultiTenantPrincipal principal;
  private final ApiKeys apiKey;

  public ApiSensorBuilder(Metrics metrics, MultiTenantPrincipal principal, ApiKeys apiKey) {
    this.metrics = metrics;
    this.principal = principal;
    this.apiKey = apiKey;
  }

  public ApiSensors build() {
    Map<String, Sensor> sensors = getOrCreateRequestResponseSensors();
    return new ApiSensors(sensors.get(REQUEST_RATE),
        sensors.get(REQUEST_BYTE_RATE),
        sensors.get(RESPONSE_BYTE_RATE),
        sensors.get(RESPONSE_TIME_NANOS));
  }

  public void addErrorSensors(ApiSensors apiSensors, Set<Errors> errors) {
    apiSensors.addErrorSensors(getOrCreateErrorSensors(errors));
  }

  private String sensorSuffix(MultiTenantPrincipal principal, ApiKeys apiKey) {
    return String.format(":%s-%s:%s-%s:%s-%s", REQUEST_TAG, apiKey.name,
        TENANT_TAG, principal.tenantMetadata().tenantName, USER_TAG, principal.getName());
  }

  private Map<String, Sensor> getOrCreateRequestResponseSensors() {
    String sensorSuffix = sensorSuffix(principal, apiKey);
    Map<String, String> sensorsToFind = new HashMap<>();
    for (String name : requestResponseSensorCreators.keySet()) {
      sensorsToFind.put(name, name + sensorSuffix);
    }
    return getOrCreateSensors(sensorsToFind, requestResponseSensorCreators);
  }

  private Map<Errors, Sensor> getOrCreateErrorSensors(Set<Errors> errors) {
    String sensorSuffix = sensorSuffix(principal, apiKey);
    Map<Errors, String> sensorsToFind = new HashMap<>();
    for (Errors error: errors) {
      String sensorName = String.format("%s:%s-%s%s", ERROR_COUNT, ERROR_TAG, error.name(),
          sensorSuffix);
      sensorsToFind.put(error, sensorName);
    }
    return getOrCreateSensors(sensorsToFind, errorSensorCreators);
  }

  private <T> Map<T, Sensor> getOrCreateSensors(Map<T, String> sensorsToFind,
      Map<T, AbstractSensorCreator> sensorCreators) {
    Map<T, Sensor> sensors;
    lock.readLock().lock();
    try {
      sensors = findSensors(metrics, sensorsToFind);
      sensorsToFind.keySet().removeAll(sensors.keySet());
    } finally {
      lock.readLock().unlock();
    }

    if (!sensorsToFind.isEmpty()) {
      lock.writeLock().lock();
      try {
        Map<T, Sensor> existingSensors = findSensors(metrics, sensorsToFind);
        sensorsToFind.keySet().removeAll(existingSensors.keySet());
        sensors.putAll(existingSensors);

        for (Map.Entry<T, String> entry : sensorsToFind.entrySet()) {
          T key = entry.getKey();
          String sensorName = entry.getValue();
          Sensor sensor = sensorCreators.get(key).createSensor(metrics, sensorName,
              principal, apiKey);
          sensors.put(key, sensor);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
    return sensors;
  }

  private <T> Map<T, Sensor> findSensors(Metrics metrics, Map<T, String> sensorNames) {
    Map<T, Sensor> existingSensors = new HashMap<>();
    for (Map.Entry<T, String> entry : sensorNames.entrySet()) {
      Sensor sensor = metrics.getSensor(entry.getValue());
      if (sensor != null) {
        existingSensors.put(entry.getKey(), sensor);
      }
    }
    return existingSensors;
  }

  private abstract static class AbstractSensorCreator {
    protected final String name;
    protected final String descriptiveName;

    AbstractSensorCreator(String name, String descriptiveName) {
      this.name = name;
      this.descriptiveName = descriptiveName;
    }

    abstract Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
        ApiKeys apiKey);

    protected Sensor createSensor(Metrics metrics, String sensorName) {
      return metrics.sensor(sensorName, metrics.config(), EXPIRY_SECONDS);
    }

    Map<String, String> metricTags(MultiTenantPrincipal principal, ApiKeys apiKey) {
      Map<String, String> tags = new HashMap<>();
      tags.put(REQUEST_TAG, apiKey.name);
      tags.put(TENANT_TAG, principal.tenantMetadata().tenantName);
      tags.put(USER_TAG, principal.getName());
      return tags;
    }

    protected Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
        String baseName, String descriptiveName) {
      MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
          String.format("The number of %s per second", descriptiveName), metricTags);
      MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
          String.format("The total number of %s", descriptiveName), metricTags);
      return new Meter(rateMetricName, totalMetricName);
    }
  }

  private static class RequestSensorCreator extends AbstractSensorCreator {
    private final boolean toCreateMinMaxAvgMetrics;
    private final boolean toCreateMeterMetrics;

    RequestSensorCreator(String name, String descriptiveName, boolean createMinMaxAvgMetrics,
                         boolean createMeterMetrics) {
      super(name, descriptiveName);
      toCreateMinMaxAvgMetrics = createMinMaxAvgMetrics;
      toCreateMeterMetrics = createMeterMetrics;
    }

    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
                        ApiKeys apiKey) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = metricTags(principal, apiKey);

      if (toCreateMeterMetrics) {
        sensor.add(createMeter(metrics, GROUP, metricTags, name, descriptiveName));
      }
      if (toCreateMinMaxAvgMetrics) {
        sensor.add(metrics.metricName(name + "-min", GROUP,
                "The minimum time taken for " + descriptiveName, metricTags),
                new Min());
        sensor.add(metrics.metricName(name + "-avg", GROUP,
                "The average time taken for " + descriptiveName, metricTags),
                new Avg());
        sensor.add(metrics.metricName(name + "-max", GROUP,
                "The maximum time taken for " + descriptiveName, metricTags),
                new Max());
      }

      return sensor;
    }
  }

  private static class RequestMeterSensorCreator extends RequestSensorCreator {
    RequestMeterSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName, false, true);
    }
  }

  private static class RequestMinMaxAvgSensorCreator extends RequestSensorCreator {
    RequestMinMaxAvgSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName, true, false);
    }
  }


  private static class ErrorCountSensorCreator extends AbstractSensorCreator {
    private final Errors error;

    ErrorCountSensorCreator(Errors error) {
      super(ERROR_COUNT, "errors");
      this.error = error;
    }

    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
        ApiKeys apiKey) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = metricTags(principal, apiKey);
      metricTags.put(ERROR_TAG, error.name());
      sensor.add(createMeter(metrics, GROUP, metricTags, name, descriptiveName));
      return sensor;
    }
  }
}
