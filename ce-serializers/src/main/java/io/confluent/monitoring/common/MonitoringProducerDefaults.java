/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

public class MonitoringProducerDefaults {

  // ensure that our production is replicated
  static final String ACKS_CONFIG = "all";
  static final String COMPRESSION_TYPE_CONFIG = "lz4";
  static final String INTERCEPTOR_CLASSES_CONFIG = "";
  static final String KEY_SERIALIZER_CLASS_CONFIG =
      "org.apache.kafka.common.serialization.ByteArraySerializer";
  static final String LINGER_MS_CONFIG = "500";
  // ensure that our requests are accepted in order
  static final int MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 1;
  // default is 0, we would like to keep trying if possible
  static final int RETRIES_CONFIG = 10;
  static final long RETRY_BACKOFF_MS_CONFIG = 500;
  static final String VALUE_SERIALIZER_CLASS_CONFIG =
      "org.apache.kafka.common.serialization.ByteArraySerializer";

  // increase default message size:
  // - metrics reporter messages can grow large with many topics / partitions
  // - stream monitoring state-store changelog messages can grow quite large at weekly granularity
  public static final int MAX_REQUEST_SIZE = 10_485_760; // 10 MB


  public static final Map<String, Object> PRODUCER_CONFIG_DEFAULTS =
      ImmutableMap.<String, Object>builder()
          .put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG)
          .put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_CONFIG)
          .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG)
          .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG)
          .put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG)
          .put(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG)
          .put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, INTERCEPTOR_CLASSES_CONFIG)
          .put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS_CONFIG)
          .put(
              ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
              MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
          ).put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_REQUEST_SIZE)
          .build();
}
