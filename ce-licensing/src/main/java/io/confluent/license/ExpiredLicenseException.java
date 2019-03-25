/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

public class ExpiredLicenseException extends InvalidLicenseException {

  private final License expired;

  public ExpiredLicenseException(License expired, String message) {
    super(message);
    this.expired = expired;
  }

  public ExpiredLicenseException(License expired, String message, Throwable cause) {
    super(message, cause);
    this.expired = expired;
  }

  public License getLicense() {
    return expired;
  }
}
