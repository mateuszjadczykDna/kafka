/*
 * Copyright [2016 - 2016] Confluent Inc.
 */
package io.confluent.serializers;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StringSerdeTest {
  StringSerde serde;
  private static final String JSON = "\"test\"";

  @Before
  public void before() {
    serde = new StringSerde();
  }

  @Test
  public void fromJSON() throws Exception {
    byte[] bytes = serde.fromJson(JSON);
    byte[] testBytes = "test".getBytes(StandardCharsets.UTF_8);
    assertEquals(StringSerde.MAGIC_BYTE_STRING, bytes[0]);
    assertEquals(testBytes.length + 1, bytes.length);
    assertArrayEquals(testBytes, Arrays.copyOfRange(bytes, 1, bytes.length));

    String deserialized = serde.deserialize(bytes);
    assertEquals("test", deserialized);
  }

  @Test
  public void toJSON() throws Exception {
    String json = serde.toJson("test");
    assertEquals(JSON, json);
  }

  @Test
  public void testDeserializeNull() throws Exception {
    assertNull(serde.deserialize(null));
  }

  @Test(expected = SerializationException.class)
  public void testDeserializeWrongMagicBytes() throws Exception {
    byte[] badBytes = new byte[] {UberSerde.MAGIC_BYTE_STRING + 1, 'h', 'e', 'l', 'l', 'o'};
    serde.deserialize(badBytes);
  }
}
