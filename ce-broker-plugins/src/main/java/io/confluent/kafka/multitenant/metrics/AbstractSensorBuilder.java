package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class AbstractSensorBuilder<S> {
  private static final ReadWriteLock lock = new ReentrantReadWriteLock();

  protected final Metrics metrics;
  protected final MultiTenantPrincipal principal;

  public AbstractSensorBuilder(Metrics metrics, MultiTenantPrincipal principal) {
    this.metrics = metrics;
    this.principal = principal;
  }

  /**
   * Returns an object which provides a way to record values in the sensors
   */
  public abstract S build();

  protected abstract String sensorSuffix(String name, MultiTenantPrincipal principal);

  protected abstract Map<String, ? extends AbstractSensorCreator> sensorCreators();

  Map<String, Sensor> getOrCreateSuffixedSensors() {
    Map<String, String> sensorsToFind = new HashMap<>();
    for (String name : sensorCreators().keySet()) {
      sensorsToFind.put(name, name + sensorSuffix(name, principal));
    }
    return getOrCreateSensors(sensorsToFind, sensorCreators());
  }

  <T> Map<T, Sensor> getOrCreateSensors(Map<T, String> sensorsToFind,
                                        Map<T, ? extends AbstractSensorCreator> sensorCreators) {
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
          sensors.put(key, createSensor(sensorCreators, key, sensorName));
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
    return sensors;
  }

  abstract <T> Sensor createSensor(Map<T, ? extends AbstractSensorCreator> sensorCreators,
                          T sensorKey, String sensorName);

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
}
