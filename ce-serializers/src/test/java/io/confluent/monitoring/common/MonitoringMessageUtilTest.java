/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

import org.junit.Test;

import io.confluent.monitoring.record.Monitoring;
import io.confluent.monitoring.record.Monitoring.MonitoringMessage;

import static org.junit.Assert.assertEquals;

public class MonitoringMessageUtilTest {

  @Test
  public void partitionForRegularMessage() throws Exception {
    assertEquals(13, MonitoringMessageUtil.partitionFor(MonitoringMessage.getDefaultInstance(), 17));
    assertEquals(14, MonitoringMessageUtil.partitionFor(MonitoringMessage.newBuilder().setClusterId("xyz").build(), 17));
    assertEquals(1806, MonitoringMessageUtil.partitionFor(MonitoringMessage.getDefaultInstance(), 2221));
  }

  @Test
  public void partitionForCONTROLCENTERMessage() throws Exception {
    assertEquals(0, MonitoringMessageUtil.partitionFor(MonitoringMessage.newBuilder()
        .setClientType(Monitoring.ClientType.CONTROLCENTER)
        .setClientId("xyz").build(),
        17
    ));
    assertEquals(12, MonitoringMessageUtil.partitionFor(MonitoringMessage.newBuilder().setClientType(Monitoring.ClientType.CONTROLCENTER).setPartition(12).build(), 17));
    assertEquals(2, MonitoringMessageUtil.partitionFor(MonitoringMessage.newBuilder().setClientType(Monitoring.ClientType.CONTROLCENTER).setPartition(12).build(), 5));
  }
}
