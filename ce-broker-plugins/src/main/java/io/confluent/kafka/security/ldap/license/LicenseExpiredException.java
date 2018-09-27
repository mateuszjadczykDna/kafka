// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.license;

import org.apache.kafka.common.KafkaException;

public class LicenseExpiredException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public LicenseExpiredException(String message) {
    super(message);
  }
}
