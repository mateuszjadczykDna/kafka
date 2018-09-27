/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Partial implementation of the HBase OrderedBytes encoding scheme -
 * https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/util/OrderedBytes.html
 *
 * <p>These byte arrays maintain the sort order of the original data structures
 * so they can be used for scans in RocksDB
 *  - Each type begins with a byte marker
 *  - Nulls are allowed and they sort first before any other type
 *  - Strings are UTF-8 encoded with a null byte end marker
 *  - Fixed-length integers are encoded big endian
 *
 * <p>Rather than drag in hbase-common and it's dependencies, we're opting to
 * implement a subset of the spec
 */
public class OrderedBytes {
  public static final int NULL_SIZE = 1;
  public static final int FIXED_INT_16_SIZE = 3;
  public static final int FIXED_INT_32_SIZE = 5;
  public static final int FIXED_INT_64_SIZE = 9;

  //https://tools.ietf.org/html/rfc362
  //characters from the U+0000..U+10FFFF range (the UTF-16
  // accessible range) are encoded using sequences of 1 to 4 octets.
  private static final int MAX_UTF8_BYTES = 4;
  public static final byte MARKER_NULL = 0x05;
  public static final byte MARKER_START_STRING = 0x34;
  public static final byte MARKER_END_STRING = 0x00;
  public static final byte MARKER_START_FIXED_INT_16 = 0x2a;
  public static final byte MARKER_START_FIXED_INT_32 = 0x2b;
  public static final byte MARKER_START_FIXED_INT_64 = 0x2c;

  public static int writeNull(ByteBuffer buffer) {
    buffer.put(MARKER_NULL);
    return NULL_SIZE;
  }

  public static boolean startsWith(byte[] array, byte[] prefix) {
    if (array.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (array[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  public static int getMaxNumBytes(String string) {
    return string.length() * MAX_UTF8_BYTES + 2;
  }

  public static String readString(ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker == MARKER_NULL) {
      return null;
    }
    if (marker != MARKER_START_STRING) {
      throw new SerializationException("Unexpected type marker for string: " + marker);
    }
    int len = getStringLength(buffer);
    byte[] strBytes = new byte[len];
    buffer.get(strBytes, buffer.arrayOffset(), len);
    byte endMarker = buffer.get();
    assert endMarker == MARKER_END_STRING;
    return new String(strBytes, StandardCharsets.UTF_8);
  }

  private static int getStringLength(ByteBuffer buffer) {
    byte[] bytes = buffer.array();
    for (int len = 0; len <= buffer.remaining(); len++) {
      if (bytes[buffer.arrayOffset() + buffer.position() + len] == MARKER_END_STRING) {
        return len;
      }
    }
    throw new SerializationException("No end of string found");
  }

  public static int writeString(ByteBuffer buffer, String value) {
    if (value == null) {
      return writeNull(buffer);
    }
    int start = buffer.position();
    buffer.put(MARKER_START_STRING);
    buffer.put(value.getBytes(StandardCharsets.UTF_8));
    buffer.put(MARKER_END_STRING);
    int numBytes = buffer.position() - start;
    assert numBytes <= getMaxNumBytes(value);
    return numBytes;
  }

  public static Integer readInt(ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker == MARKER_NULL) {
      return null;
    }
    if (marker != MARKER_START_FIXED_INT_32) {
      throw new SerializationException("Unexpected type marker for int: " + marker);
    }
    buffer.order(ByteOrder.BIG_ENDIAN);
    return flipSignBit(buffer.getInt());
  }

  /**
   * Appends value to buffer, using (value ^ Integer.MIN_VALUE) to
   * invert to sign bit so that the appended bytes will sort correctly
   *
   * @param buffer to append to
   * @param value to append
   * @return # of bytes written to buffer
   */
  public static int writeInt(ByteBuffer buffer, Integer value) {
    if (value == null) {
      return writeNull(buffer);
    }
    final int start = buffer.position();
    buffer.put(MARKER_START_FIXED_INT_32);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(flipSignBit(value));
    assert (buffer.position() - start) == FIXED_INT_32_SIZE;
    return FIXED_INT_32_SIZE;
  }

  public static Long readLong(ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker == MARKER_NULL) {
      return null;
    }
    if (marker != MARKER_START_FIXED_INT_64) {
      throw new SerializationException("Unexpected type marker for long: " + marker);
    }
    buffer.order(ByteOrder.BIG_ENDIAN);
    return flipSignBit(buffer.getLong());
  }

  /**
   * Appends value to buffer, using (value ^ Long.MIN_VALUE) to
   * invert to sign bit so that the appended bytes will sort correctly
   *
   * @param buffer to append to
   * @param value to append
   * @return # of bytes written to buffer
   */
  public static int writeLong(ByteBuffer buffer, Long value) {
    if (value == null) {
      return writeNull(buffer);
    }
    final int start = buffer.position();
    buffer.put(MARKER_START_FIXED_INT_64);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putLong(flipSignBit(value));
    assert (buffer.position() - start) == FIXED_INT_64_SIZE;
    return FIXED_INT_64_SIZE;
  }

  public static Short readShort(ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker == MARKER_NULL) {
      return null;
    }
    if (marker != MARKER_START_FIXED_INT_16) {
      throw new SerializationException("Unexpected type marker for short: " + marker);
    }
    buffer.order(ByteOrder.BIG_ENDIAN);
    return flipSignBit(buffer.getShort());
  }

  /**
   * Appends value to buffer, using (value ^ Short.MIN_VALUE) to
   * invert to sign bit so that the appended bytes will sort correctly
   *
   * @param buffer to append to
   * @param value to append
   * @return # of bytes written to buffer
   */
  public static int writeShort(ByteBuffer buffer, Short value) {
    if (value == null) {
      return writeNull(buffer);
    }
    final int start = buffer.position();
    buffer.put(MARKER_START_FIXED_INT_16);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putShort(flipSignBit(value));
    assert (buffer.position() - start) == FIXED_INT_16_SIZE;
    return FIXED_INT_16_SIZE;
  }

  public static long flipSignBit(long in) {
    return in ^ Long.MIN_VALUE;
  }

  public static int flipSignBit(int in) {
    return in ^ Integer.MIN_VALUE;
  }

  public static short flipSignBit(short in) {
    return (short) (in ^ Short.MIN_VALUE);
  }
}
