/*
 * Copyright [2016 - 2016] Confluent Inc.
 */
package io.confluent.serializers;

import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class OrderedBytesTest {
  @Test
  public void startsWith() throws Exception {
    byte[] bytes = DatatypeConverter.parseHexBinary("7E2A000134746573742D746F706963002B0000000034");
    byte[] tooLong = DatatypeConverter.parseHexBinary("7E2A000134746573742D746F706963002B0000000034746573742D636C69656E74003453657373696F6E2D6F6E6500");
    byte[] prefix = DatatypeConverter.parseHexBinary("7E2A000134746573742D746F706963002B");
    byte[] nonPrefix = DatatypeConverter.parseHexBinary("7E2A000134746573742D746F706963002C");

    assertTrue(OrderedBytes.startsWith(bytes, bytes));
    assertFalse(OrderedBytes.startsWith(bytes, tooLong));
    assertTrue(OrderedBytes.startsWith(bytes, bytes));
    assertTrue(OrderedBytes.startsWith(bytes, prefix));
    assertFalse(OrderedBytes.startsWith(bytes, nonPrefix));
  }

}
