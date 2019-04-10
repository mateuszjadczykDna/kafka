/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.common.security.utils;

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.util.io.pem.PemWriter;
import org.bouncycastle.openssl.PEMKeyPair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.security.KeyPair;
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

  public static void writePublicKey(OutputStream out, PublicKey key) throws IOException {
    try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(out, "utf8"))) {
      pemWriter.writeObject(new JcaMiscPEMGenerator(key));
    }
  }

  public static KeyPair loadKeyPair(InputStream inputStream) throws IOException {
    try (InputStreamReader reader = new InputStreamReader(inputStream, US_ASCII))  {
      PEMParser pemParser = new PEMParser(new BufferedReader(reader));
      PEMKeyPair pemKeyPair = (PEMKeyPair) pemParser.readObject();
      return new JcaPEMKeyConverter().getKeyPair(pemKeyPair);
    }
  }

  public static void writeKeyPair(OutputStream out, KeyPair keyPair) throws IOException {
    try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(out, "utf8"))) {
      pemWriter.writeObject(new JcaMiscPEMGenerator(keyPair));
    }
  }
}
