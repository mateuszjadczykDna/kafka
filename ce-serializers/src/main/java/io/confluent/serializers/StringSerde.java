/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public class StringSerde implements UberSerde<String> {
  private static final Logger log = LoggerFactory.getLogger(StringSerde.class);
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private final Gson gson = new Gson();

  @Override
  public Class<String> type() {
    return String.class;
  }

  @Override
  public byte[] serialize(String string) {
    if (string == null) {
      return null;
    }
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE_STRING);
      out.write(string.getBytes(CHARSET));
      return out.toByteArray();
    } catch (Exception e) {
      String errMsg = "Error serializing string message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public byte[] serialize(String topic, String string) {
    return serialize(string);
  }

  @Override
  public String deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      byte magic = buffer.get();
      if (MAGIC_BYTE_STRING != magic) {
        String errMsg = String.format(
            "Tried to deserialize message with unknown magic byte %s",
            magic
        );
        log.error(errMsg);
        throw new SerializationException(errMsg);
      }
      return new String(Arrays.copyOfRange(buffer.array(), 1, buffer.array().length), CHARSET);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing string message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public String deserialize(String topic, byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public Serializer<String> serializer() {
    return this;
  }

  @Override
  public Deserializer<String> deserializer() {
    return this;
  }

  @Override
  public void close() {
  }

  @Override
  public byte[] fromJson(String json) {
    String string = gson.fromJson(json, String.class);
    return serialize(string);
  }

  @Override
  public String toJson(String string) {
    return gson.toJson(new JsonPrimitive(string));
  }
}
