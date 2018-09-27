/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UberSerdes {

  public static final BaseEncoding BASE_64 = BaseEncoding.base64();

  public static UberSerde<Long> longs() {
    return new DelegatingUberSerde<Long>(Serdes.Long()) {
      @Override
      public Class<Long> type() {
        return Long.class;
      }

      @Override
      public String toJson(Long message) {
        return new Gson().toJson(message);
      }

      @Override
      public byte[] fromJson(String json) {
        return serialize(new Gson().fromJson(json, Long.class));
      }
    };
  }

  public static <T> UberSerde<T> fromSerde(Serde<T> serde, final Class<T> clazz) {

    return new DelegatingUberSerde<T>(serde) {
      @Override
      public Class<T> type() {
        return clazz;
      }

      @Override
      public byte[] fromJson(String json) {
        return BASE_64.decode(new Gson().fromJson(json, String.class));
      }

      @Override
      public String toJson(T message) {
        return new Gson().toJson(BASE_64.encode(serialize(message)));
      }
    };
  }

  public abstract static class DelegatingUberSerde<T> implements UberSerde<T> {
    private final Serde<T> serde;

    public DelegatingUberSerde(Serde<T> serde) {
      this.serde = serde;
    }

    @Override
    public byte[] serialize(T message) {
      return serde.serializer().serialize(null,  message);
    }

    @Override
    public byte[] serialize(String topic, T data) {
      return serde.serializer().serialize(topic, data);
    }

    @Override
    public T deserialize(byte[] bytes) {
      return serde.deserializer().deserialize(null, bytes);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
      return serde.deserializer().deserialize(topic, data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      serde.configure(configs, isKey);
    }

    @Override
    public void close() {
      serde.close();
    }

    @Override
    public Serializer<T> serializer() {
      return serde.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
      return serde.deserializer();
    }
  }
}
