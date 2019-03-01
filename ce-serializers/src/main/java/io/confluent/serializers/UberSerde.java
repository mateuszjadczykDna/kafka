/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public interface UberSerde<T> extends SerdeWithJson<T>, Serde<T>, Deserializer<T>, Serializer<T> {

  byte MAGIC_BYTE_PROTOBUF = Byte.MAX_VALUE;
  byte MAGIC_BYTE_ORDERED_KEY = MAGIC_BYTE_PROTOBUF - (byte) 1;
  byte MAGIC_BYTE_STRING = MAGIC_BYTE_ORDERED_KEY - (byte) 1;

  Class<T> type();

  /**
   * Configure this class, which will configure the underlying serializer and deserializer.
   *
   * @param configs configs in key/value pairs
   * @param isKey whether is for key or value
   */
  default void configure(Map<String, ?> configs, boolean isKey) {
    // intentionally left blank
  }

  /**
   * Close this deserializer.
   * <p>
   * This method must be idempotent as it may be called multiple times.
   */
  @Override
  default void close() {
    // intentionally left blank
  }
}
