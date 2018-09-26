/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.common.security.util;

import org.junit.Test;

import java.io.InputStream;
import java.security.PublicKey;

import static org.junit.Assert.assertEquals;

public class PemUtilsTest {

  @Test
  public void loadPublicKey() throws Exception {
    InputStream resourceAsStream = this.getClass().getResourceAsStream("/publickey.pem");
    PublicKey publicKey = PemUtils.loadPublicKey(resourceAsStream);
    assertEquals("RSA", publicKey.getAlgorithm());
    assertEquals("X.509", publicKey.getFormat());
  }
}
