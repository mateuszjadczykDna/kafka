// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.license;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.confluent.license.License;
import io.confluent.license.trial.ZkTrialPeriod;
import java.security.PublicKey;
import java.util.Date;
import java.util.Locale;
import org.apache.kafka.common.utils.Time;
import org.jose4j.jwt.JwtClaims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LicenseValidator {
  private static final Logger log = LoggerFactory.getLogger(LicenseValidator.class);

  private static final PublicKey PUBLIC_KEY;
  private static final long EXPIRY_LOG_INTERVAL_MS = 10000;
  private static final String METRIC_GROUP = "kafka.ldap.plugins";
  private static final String METRIC_NAME = "licenseStatus";

  public enum LicenseStatus {
    TRIAL,
    TRIAL_EXPIRED,
    LICENSE_ACTIVE,
    LICENSE_EXPIRED
  }

  private final Time time;
  private final boolean licenseConfigured;
  private final long validUntilMs;
  private final MetricName licenseStatusMetricName;
  private LicenseStatus licenseStatus;
  private long lastExpiryErrorLogMs;

  static {
    PublicKey publicKey = null;
    try {
      publicKey = License.loadPublicKey();
    } catch (Exception e) {
      log.error("Public key for license service could not be loaded", e);
    }
    PUBLIC_KEY = publicKey;
  }


  public LicenseValidator(String license, String zkConnect, Time time) {
    if (PUBLIC_KEY == null) {
      throw new InvalidLicenseException("Public key for license validator could not be loaded");
    }

    this.time = time;
    long now = time.milliseconds();
    licenseConfigured = license != null && !license.isEmpty();

    if (licenseConfigured) {
      try {
        JwtClaims claims = License.verify(PUBLIC_KEY, license);
        validUntilMs = claims.getExpirationTime().getValueInMillis();
        licenseStatus = LicenseStatus.LICENSE_ACTIVE;
      } catch (Exception e) {
        String errorMessage = "Validation of configured license failed";
        log.error(errorMessage, e);
        throw new InvalidLicenseException(errorMessage, e);
      }
    } else {
      ZkTrialPeriod trialPeriod = new ZkTrialPeriod(zkConnect);
      validUntilMs = now + trialPeriod.startOrVerify(now);
      licenseStatus = LicenseStatus.TRIAL;
    }

    verifyLicense(true);
    licenseStatusMetricName = registerMetric();
  }

  public void verifyLicense(boolean failOnError) {
    long now = time.milliseconds();
    if (now >= validUntilMs) {
      String errorMessage;
      if (licenseConfigured) {
        errorMessage = String.format("Your license expired at %s. "
            + "Please add a valid license to continue using the product", new Date(validUntilMs));
        licenseStatus = LicenseStatus.LICENSE_EXPIRED;
      } else {
        errorMessage = "Your trial license has expired. "
            + "Please add a valid license to continue using the product";
        licenseStatus = LicenseStatus.TRIAL_EXPIRED;
      }
      if (failOnError || now - lastExpiryErrorLogMs > EXPIRY_LOG_INTERVAL_MS) {
        log.error(errorMessage);
        lastExpiryErrorLogMs = now;
      }
      if (failOnError) {
        throw new LicenseExpiredException(errorMessage);
      }
    }
  }

  public void close() {
    Metrics.defaultRegistry().removeMetric(licenseStatusMetricName);
  }

  // Registering yammer metric since we don't have access to the KafkaMetrics instance
  private MetricName registerMetric() {
    String metricType = LicenseValidator.class.getSimpleName();
    MetricName metricName = new MetricName(METRIC_GROUP, metricType, METRIC_NAME, null,
        String.format("%s:type=%s,name=%s", METRIC_GROUP, metricType, METRIC_NAME));
    Metrics.defaultRegistry().newGauge(metricName, new Gauge<String>() {
      @Override
      public String value() {
        return licenseStatus.name().toLowerCase(Locale.ROOT);
      }
    });
    return metricName;
  }
}