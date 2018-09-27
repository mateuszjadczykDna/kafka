/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;

public class OrderedKeyUberSerdeDelegate<T> implements OrderedKeyUberSerde<T> {

  private final OrderedKeyUberSerde<T> delegate;

  public OrderedKeyUberSerdeDelegate(OrderedKeyUberSerde<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public OrderedKeyUberSerde<T> prefixKeySerde(int numFields) {
    return delegate.prefixKeySerde(numFields);
  }

  @Override
  public int numFields() {
    return delegate.numFields();
  }

  @Override
  public Bytes key(T message) {
    return delegate.key(message);
  }

  @Override
  public T toProto(Bytes key) {
    return delegate.toProto(key);
  }

  @Override
  public String toHexString(Bytes key) {
    return delegate.toHexString(key);
  }

  @Override
  public Class<Bytes> type() {
    return delegate.type();
  }

  @Override
  public Bytes deserialize(byte[] bytes) {
    return delegate.deserialize(bytes);
  }

  @Override
  public Bytes deserialize(String s, byte[] bytes) {
    return delegate.deserialize(s, bytes);
  }

  @Override
  public byte[] serialize(Bytes message) {
    return delegate.serialize(message);
  }

  @Override
  public byte[] serialize(String s, Bytes bytes) {
    return delegate.serialize(s, bytes);
  }

  @Override
  public byte[] fromJson(String json) {
    return delegate.fromJson(json);
  }

  @Override
  public String toJson(Bytes message) {
    return delegate.toJson(message);
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    delegate.configure(map, b);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Serializer<Bytes> serializer() {
    return delegate.serializer();
  }

  @Override
  public Deserializer<Bytes> deserializer() {
    return delegate.deserializer();
  }
}
