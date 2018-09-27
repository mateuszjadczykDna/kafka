// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.license;

import org.apache.kafka.common.KafkaException;

public class InvalidLicenseException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public InvalidLicenseException(String message) {
    super(message);
  }

  public InvalidLicenseException(String message, Throwable cause) {
    super(message, cause);
  }
}
