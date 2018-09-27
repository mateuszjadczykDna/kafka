/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public interface UberSerde<T> extends SerdeWithJson<T>, Serde<T>, Deserializer<T>, Serializer<T> {

  byte MAGIC_BYTE_PROTOBUF = Byte.MAX_VALUE;
  byte MAGIC_BYTE_ORDERED_KEY = MAGIC_BYTE_PROTOBUF - (byte) 1;
  byte MAGIC_BYTE_STRING = MAGIC_BYTE_ORDERED_KEY - (byte) 1;

  Class<T> type();
}
