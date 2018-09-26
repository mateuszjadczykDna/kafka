/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.common.security.util;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.security.PublicKey;

public class PemUtils {

  private static final Charset US_ASCII = Charset.forName("US-ASCII");

  public static PublicKey loadPublicKey(InputStream inputStream) throws IOException {
    try (InputStreamReader reader = new InputStreamReader(inputStream, US_ASCII)) {
      PEMParser pemParser = new PEMParser(new BufferedReader(reader));
      SubjectPublicKeyInfo keyInfo = SubjectPublicKeyInfo.getInstance(pemParser.readObject());
      return new JcaPEMKeyConverter().getPublicKey(keyInfo);
    }
  }
}
