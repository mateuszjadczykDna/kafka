/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.utils.Bytes;

public interface OrderedKeyUberSerde<T> extends UberSerde<Bytes> {

  OrderedKeyUberSerde<T> prefixKeySerde(int numFields);

  int numFields();

  Bytes key(T message);

  T toProto(Bytes key);

  String toHexString(Bytes key);

}
