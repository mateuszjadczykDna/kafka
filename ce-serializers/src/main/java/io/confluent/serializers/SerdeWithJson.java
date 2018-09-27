/*
 * Copyright [2017 - 2018] Confluent Inc.
 */

package io.confluent.serializers;

public interface SerdeWithJson<T> {
  byte[] serialize(T message);

  T deserialize(byte[] bytes);

  byte[] fromJson(String json);

  String toJson(T message);
}
