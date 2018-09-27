/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

public class InvalidLicenseException extends Exception {
  InvalidLicenseException(String message) {
    super(message);
  }

  InvalidLicenseException(String message, Throwable cause) {
    super(message, cause);
  }
}
