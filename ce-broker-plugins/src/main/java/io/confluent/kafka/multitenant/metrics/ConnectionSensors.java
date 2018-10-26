// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import org.apache.kafka.common.metrics.Sensor;

public class ConnectionSensors {
  protected final Sensor authRate;
  protected final Sensor activeAuthenticatedConnectionsCount;

  public ConnectionSensors(Sensor authRate, Sensor activeAuthenticatedConnectionsCount) {
    this.authRate = authRate;
    this.activeAuthenticatedConnectionsCount = activeAuthenticatedConnectionsCount;
  }

  public void recordAuthenticatedConnection() {
    this.authRate.record();
    this.activeAuthenticatedConnectionsCount.record();
  }

  public void recordAuthenticatedDisconnection() {
    this.activeAuthenticatedConnectionsCount.record(-1);
  }
}
