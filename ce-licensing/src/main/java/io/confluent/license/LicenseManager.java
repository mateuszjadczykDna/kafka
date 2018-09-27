/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.common.utils.Time;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.license.util.StringUtils;

public class LicenseManager {

  private static final Logger log = LoggerFactory.getLogger(LicenseManager.class);
  protected static final long WARN_THRESHOLD_MILLIS = TimeUnit.DAYS.toMillis(5);

  private final String topic;
  private final LicenseStore licenseStore;
  private final Time time;

  public LicenseManager(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig
  ) {
    this.topic = topic;
    this.licenseStore = new LicenseStore(this.topic, producerConfig, consumerConfig, topicConfig);
    this.time = Time.SYSTEM;
    this.licenseStore.start();
  }

  //visible for testing
  protected LicenseManager(
      String topic,
      LicenseStore licenseStore,
      Time time
  ) {
    this.topic = topic;
    this.licenseStore = licenseStore;
    this.time = time;
    this.licenseStore.start();
  }

  public void stop() {
    licenseStore.stop();
  }

  public void registerOrValidateLicense(String license) throws InvalidLicenseException {
    PublicKey publicKey = loadPublicKey();
    JwtClaims givenJwtClaims = null;

    // If a license key is given, always validate just against this key and fail if it's not valid
    if (StringUtils.isNotBlank(license)) {
      try {
        givenJwtClaims = License.verify(publicKey, license);
      } catch (InvalidJwtException e) {
        throw new InvalidLicenseException("Invalid license.", e);
      }
    }

    long now = time.milliseconds();
    long storedExpiration = Long.MIN_VALUE;
    long givenExpiration = Long.MIN_VALUE;
    long maxExpiration = Long.MIN_VALUE;
    String licenseToRegister = license;
    String storedLicense = licenseStore.licenseScan();
    if (StringUtils.isBlank(storedLicense)) {
      // No license, trial or other was stored before
      if (StringUtils.isBlank(license)) {
        // Starting trial period.
        // Expiration is rounded to seconds during the construction of baseClaims, thus add a sec
        givenJwtClaims = License.baseClaims("trial", now + TimeUnit.DAYS.toMillis(30) + 1000, true);
        licenseToRegister = License.generateTrialLicense(givenJwtClaims);
      }
      // Get expiration for license, regular or trial
      try {
        givenExpiration = License.getExpiration(givenJwtClaims);
      } catch (Throwable t) {
        throw new InvalidLicenseException(
            "Error extracting expiration date from valid license: ", t
        );
      }
    } else {
      // A license has been registered previously
      try {
        JwtClaims storedJwtClaims = License.verifyStored(publicKey, storedLicense);
        // Get expiration of previously registered license
        storedExpiration = License.getExpiration(storedJwtClaims);
      } catch (Throwable t) {
        log.warn("Error extracting stored license.", t);
      }
      // If an error occurred maxExpiration is Long.MIN_VALUE
      maxExpiration = storedExpiration;
      if (StringUtils.isNotBlank(license)) {
        // Get expiration of given valid license.
        try {
          givenExpiration = License.getExpiration(givenJwtClaims);
        } catch (Throwable t) {
          log.warn("Error getting expiration date from valid given license. Will use already "
              + "registered license: ", t);
        }
      }
    }

    // Use license with highest expiration date
    if (givenExpiration > storedExpiration) {
      licenseStore.registerLicense(licenseToRegister);
      maxExpiration = givenExpiration;
    }

    checkExpiration(now, maxExpiration);
  }

  private static void checkExpiration(long now, long maxExpiration) throws InvalidLicenseException {
    if (now > maxExpiration) {
      // License has expired
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      throw new InvalidLicenseException(
          "License expired on : "
              + dateFormat.format(new Date(maxExpiration))
      );
    } else if (maxExpiration < Long.MAX_VALUE) {
      // License is still valid but limited
      long remainingMillis = maxExpiration - now;
      String msg = String.format(
          "Remaining time before license expires: %s days",
          TimeUnit.MILLISECONDS.toDays(remainingMillis)
      );
      if (remainingMillis < WARN_THRESHOLD_MILLIS) {
        // Issue a warning 5 days before expiration
        log.warn(msg);
      } else {
        log.info(msg);
      }
    }
  }

  public static PublicKey loadPublicKey() {
    try {
      return License.loadPublicKey();
    } catch (NoSuchAlgorithmException | IOException | InvalidKeySpecException e) {
      // This can happen for two reasons:
      //  1. The public key was not packaged with the jar.
      //  2. The public key which was packaged was corrupt.
      throw new IllegalStateException("Internal license validation error", e);
    }
  }
}
