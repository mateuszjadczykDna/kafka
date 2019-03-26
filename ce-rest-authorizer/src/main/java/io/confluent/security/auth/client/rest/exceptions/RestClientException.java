// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest.exceptions;

public class RestClientException extends Exception {

  private final int status;
  private final int errorCode;

  public RestClientException(final String message, final int status, final int errorCode) {
    super(message + "; error code: " + errorCode);
    this.status = status;
    this.errorCode = errorCode;
  }

  public int status() {
    return status;
  }

  public int errorCode() {
    return errorCode;
  }
}