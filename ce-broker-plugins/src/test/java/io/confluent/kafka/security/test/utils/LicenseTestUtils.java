// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.utils;

import io.confluent.kafka.security.ldap.license.LicenseValidator;
import io.confluent.license.License;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

public class LicenseTestUtils {

  private static final KeyPair KEY_PAIR;

  static {
    // Inject a new key-pair for License service for testing using reflection.
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048, SecureRandom.getInstance("SHA1PRNG"));
      KEY_PAIR = generator.generateKeyPair();
      Field publicKeyField = LicenseValidator.class.getDeclaredField("PUBLIC_KEY");
      publicKeyField.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      int modifiers = publicKeyField.getModifiers();
      modifiersField.setInt(publicKeyField, modifiers & ~Modifier.FINAL);
      publicKeyField.set(null, KEY_PAIR.getPublic());
      publicKeyField.setAccessible(false);
      modifiersField.setInt(publicKeyField, modifiers);
      modifiersField.setAccessible(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String generateLicense() {
    return generateLicense(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
  }

  public static String generateLicense(long expiryTimeMs) {
    try {
      return License.sign(KEY_PAIR.getPrivate(), "LDAP Plugins Test", expiryTimeMs, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
