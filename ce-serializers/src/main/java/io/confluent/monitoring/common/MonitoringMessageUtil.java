/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import io.confluent.monitoring.record.Monitoring;
import io.confluent.monitoring.record.Monitoring.MonitoringMessage;

public class MonitoringMessageUtil {

  private static final Logger log = LoggerFactory.getLogger(MonitoringMessageUtil.class);

  public static MonitoringMessage baseMonitoringMessage() {
    return MonitoringMessage.newBuilder()
        .setWindow(-1)
        .setPartition(-1)
        .setMinWindow(-1)
        .setMaxWindow(-1)
        .setSamplePeriod(-1)
        .build();
  }

  public static int partitionFor(MonitoringMessage message, int numPartitions) {
    if (!Monitoring.ClientType.CONTROLCENTER.equals(message.getClientType())) {
      return Math.abs(
          Objects.hash(
              message.getClusterId(),
              message.getClientId(),
              message.getTopic(),
              message.getPartition()
          ) % numPartitions
      );
    } else {
      // this is only expected to run in the re-partitioning step, not in the interceptors
      int partition = message.getPartition();
      if (partition >= numPartitions || partition < 0) {
        log.error(
            "partition={} invalid, expected partition >= 0 and < {}",
            partition,
            numPartitions
        );
        partition = partition % numPartitions;
      }
      return partition;
    }
  }
}
