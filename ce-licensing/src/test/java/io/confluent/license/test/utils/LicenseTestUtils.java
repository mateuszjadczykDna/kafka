// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.test.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.confluent.license.License;
import io.confluent.license.validator.ConfluentLicenseValidator;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LicenseTestUtils {

  private static final KeyPair KEY_PAIR;

  static {
    // Inject a new key-pair for License service for testing using reflection.
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048, SecureRandom.getInstance("SHA1PRNG"));
      KEY_PAIR = generator.generateKeyPair();
      Field publicKeyField = ConfluentLicenseValidator.class.getDeclaredField("PUBLIC_KEY");
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

  public static void verifyLicenseMetric(String metricGroup, LicenseStatus status) {
    Metric metric = null;
    for (Map.Entry<MetricName, Metric> entry : Metrics.defaultRegistry().allMetrics().entrySet()) {
      MetricName metricName = entry.getKey();
      if (metricGroup.equals(metricName.getGroup())) {
        assertEquals("licenseStatus", metricName.getName());
        metric = entry.getValue();
        break;
      }
    }
    assertNotNull("License metric not found", metric);
    assertEquals(status.name().toLowerCase(Locale.ROOT), ((Gauge<?>) metric).value());
  }
}
