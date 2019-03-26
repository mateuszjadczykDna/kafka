/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.common.security.utils;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PemUtilsTest {

  @Test
  public void testLoadPublicKey() throws Exception {
    InputStream resourceAsStream = this.getClass().getResourceAsStream("/publickey.pem");
    PublicKey publicKey = PemUtils.loadPublicKey(resourceAsStream);
    assertEquals("RSA", publicKey.getAlgorithm());
    assertEquals("X.509", publicKey.getFormat());
  }

  @Test
  public void testWritePublicKey() throws Exception {
    final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keyPair = generator.generateKeyPair();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PemUtils.writePublicKey(out, keyPair.getPublic());
    String outputString = out.toString("utf8");
    assertTrue(outputString.startsWith("-----BEGIN PUBLIC KEY-----\n"));
    assertTrue(outputString.endsWith("-----END PUBLIC KEY-----\n"));
  }
}
