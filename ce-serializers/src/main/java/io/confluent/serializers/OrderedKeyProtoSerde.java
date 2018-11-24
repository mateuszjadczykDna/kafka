/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * Serde to convert generated Profobuf messages into lexographically
 * sortable byte-arrays suitable for range scans in RocksDB
 */
public class OrderedKeyProtoSerde<T extends Message> implements OrderedKeyUberSerde<T> {
  protected static final Logger log = LoggerFactory.getLogger(OrderedKeyProtoSerde.class);
  protected final T instance;
  protected final ImmutableList<FieldDescriptor> fields;
  protected static final int ENUM_FIELD_SIZE = OrderedBytes.FIXED_INT_16_SIZE;

  public static <T extends Message> OrderedKeyProtoSerde<T> create(
      T instance,
      int...fieldNumbers
  ) {
    Descriptor desc = instance.getDescriptorForType();
    ImmutableList.Builder<FieldDescriptor> builder = ImmutableList.builder();
    for (int fieldNumber : fieldNumbers) {
      builder.add(desc.findFieldByNumber(fieldNumber));
    }
    return new OrderedKeyProtoSerde<>(instance, builder.build());
  }

  public static void readMagicByte(ByteBuffer buffer) {
    byte magic = buffer.get();
    if (MAGIC_BYTE_ORDERED_KEY != magic) {
      String errMsg = String.format(
          "Tried to deserialize message with unknown magic byte %s",
          magic
      );
      log.error(errMsg);
      throw new SerializationException(errMsg);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T extends Message> T readProto(
      ByteBuffer buffer,
      T instance,
      ImmutableList<FieldDescriptor> fields
  ) {
    Message.Builder builder = instance.newBuilderForType();
    for (FieldDescriptor field : fields) {
      switch (field.getJavaType()) {
        case INT:
          builder.setField(field, OrderedBytes.readInt(buffer));
          break;
        case LONG:
          builder.setField(field, OrderedBytes.readLong(buffer));
          break;
        case STRING:
          builder.setField(field, OrderedBytes.readString(buffer));
          break;
        case ENUM:
          Short enumNumber = OrderedBytes.readShort(buffer);
          if (enumNumber != null) {
            builder.setField(field, field.getEnumType().findValueByNumber(enumNumber));
          }
          break;
        default:
          throw new IllegalArgumentException("Unexpected type: " + field.getJavaType().name());
      }
    }
    return (T) builder.build();
  }

  public static void writeMagicByte(ByteBuffer buffer) {
    buffer.put(MAGIC_BYTE_ORDERED_KEY);
  }

  protected byte[] protoToBytes(
      T message,
      ImmutableList<FieldDescriptor> fields,
      int numFields
  ) {
    if (message == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.allocate(getMaxSerializedBytes(message, fields));
      writeMagicByte(buffer);
      writeProto(buffer, message, fields, numFields);
      byte[] result = new byte[buffer.position()];
      buffer.flip();
      buffer.get(result, 0, result.length);
      return result;
    } catch (Exception e) {
      String errMsg = "Error serializing key message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  public static void writeProto(
      ByteBuffer buffer,
      Message message,
      ImmutableList<FieldDescriptor> fields,
      int numFields
  ) {
    for (int i = 0; i < numFields; i++) {
      if (i > numFields) {
        break;
      }
      FieldDescriptor field = fields.get(i);
      switch (field.getJavaType()) {
        case INT:
          OrderedBytes.writeInt(buffer, (Integer) message.getField(field));
          break;
        case LONG:
          OrderedBytes.writeLong(buffer, (Long) message.getField(field));
          break;
        case STRING:
          OrderedBytes.writeString(buffer, (String) message.getField(field));
          break;
        case ENUM:
          int enumNumber = ((EnumValueDescriptor) message.getField(field)).getNumber();
          encodeEnum(buffer, enumNumber);
          break;
        default:
          throw new IllegalArgumentException("Unexpected type: " + field.getJavaType().name());
      }
    }
  }

  public static void encodeEnum(ByteBuffer buffer, int enumVal) {
    if (enumVal > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Enum value is too large: " + enumVal);
    }
    OrderedBytes.writeShort(buffer, (short) enumVal);
  }

  public static int getMaxSerializedBytes(Message message, ImmutableList<FieldDescriptor> fields) {
    int size = 1; //magic byte
    for (FieldDescriptor field : fields) {
      switch (field.getJavaType()) {
        case INT:
          size += OrderedBytes.FIXED_INT_32_SIZE;
          break;
        case LONG:
          size += OrderedBytes.FIXED_INT_64_SIZE;
          break;
        case STRING:
          size += OrderedBytes.getMaxNumBytes((String) message.getField(field));
          break;
        case ENUM:
          size += ENUM_FIELD_SIZE;
          break;
        default:
          throw new IllegalArgumentException("Unexpected type: " + field.getJavaType().name());
      }
    }
    return size;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Message> T jsonToProto(String json, T instance) {
    Message.Builder builder = instance.newBuilderForType();
    try {
      JsonFormat.parser().merge(json, builder);
      return (T) builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("JSON parsing failed", e);
    }
  }

  public static <T extends Message> String protoToJson(T message) {
    try {
      String json = JsonFormat.printer().print(message);
      //Parse and print in compact form here b/c JsonFormat doesn't support it
      Gson gson = new Gson();
      return gson.toJson(gson.fromJson(json, Object.class));
    } catch (InvalidProtocolBufferException e) {
      throw new SerializationException("JSON formatting failed", e);
    }
  }

  public OrderedKeyProtoSerde(T instance, ImmutableList<FieldDescriptor> fields) {
    this.instance = instance;
    this.fields = fields;
  }

  @Override
  public Class<Bytes> type() {
    return Bytes.class;
  }

  @Override
  public byte[] serialize(Bytes bytes) {
    return bytes.get();
  }

  @Override
  public byte[] serialize(String topic, Bytes data) {
    return this.serialize(data);
  }

  @Override
  public Bytes deserialize(String topic, byte[] data) {
    return this.deserialize(data);
  }

  @Override
  public Bytes deserialize(byte[] bytes) {
    return Bytes.wrap(bytes);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {
  }

  @Override
  public Serializer<Bytes> serializer() {
    return this;
  }

  @Override
  public Deserializer<Bytes> deserializer() {
    return this;
  }

  @Override
  public byte[] fromJson(String json) {
    T proto = jsonToProto(json, instance);
    return protoToBytes(proto, fields, fields.size());
  }

  @Override
  public String toJson(Bytes bytes) {
    T obj = toProto(bytes);
    return protoToJson(obj);
  }

  @Override
  public OrderedKeyUberSerde<T> prefixKeySerde(int numFields) {
    if (numFields <= 0) {
      throw new IllegalArgumentException(
          String.format("Fields requested %d must be greater than zero", numFields)
      );
    }
    if (numFields > fields.size()) {
      throw new IllegalArgumentException(
          String.format("Fields requested %d is higher than total %d", numFields, fields.size())
      );
    }
    return new OrderedKeyProtoSerde<>(instance, fields.subList(0, numFields));
  }

  @Override
  public int numFields() {
    return fields.size();
  }

  @Override
  public Bytes key(T message) {
    return Bytes.wrap(protoToBytes(message, fields, fields.size()));
  }

  @Override
  public T toProto(Bytes key) {
    if (key == null) {
      return null;
    }
    try {
      ByteBuffer buffer = ByteBuffer.wrap(key.get());
      readMagicByte(buffer);
      return readProto(buffer, instance, fields);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing key message";
      log.error(errMsg, e);
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public String toHexString(Bytes key) {
    return DatatypeConverter.printHexBinary(key.get());
  }
}
