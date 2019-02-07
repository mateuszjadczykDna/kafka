// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.common.license;

import org.apache.kafka.common.utils.Time;

public interface LicenseValidator {

  void initializeAndVerify(String license, String zkConnect, Time time, String metricGroup);

  void verifyLicense(boolean failOnError);

  void close();
}
