// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.license;

import io.confluent.kafka.security.test.utils.LicenseTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

public class LicenseValidatorTest {

  private final MockTime time = new MockTime();

  @Test
  public void testLicense() {
    String license = LicenseTestUtils.generateLicense();
    LicenseValidator licenseValidator = new LicenseValidator(license, null, time);
    licenseValidator.verifyLicense(true);
  }

  @Test(expected = InvalidLicenseException.class)
  public void testInvalidLicense() {
    new LicenseValidator("invalid", null, time);
  }

  @Test(expected = LicenseExpiredException.class)
  public void testExpiredLicense() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    time.sleep(licensePeriodMs + 1000);
    new LicenseValidator(license, null, time);
  }

  @Test
  public void testExpiredLicenseWithoutFailOnExpiry() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    LicenseValidator licenseValidator = new LicenseValidator(license, null, time);
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.verifyLicense(false); // should not throw exception
  }

  @Test(expected = LicenseExpiredException.class)
  public void testExpiredLicenseWithFailOnExpiry() {
    long licensePeriodMs =  60 * 60 * 1000;
    String license = LicenseTestUtils.generateLicense(time.milliseconds() + licensePeriodMs);
    LicenseValidator licenseValidator = new LicenseValidator(license, null, time);
    time.sleep(licensePeriodMs + 1000);
    licenseValidator.verifyLicense(true);
  }
}