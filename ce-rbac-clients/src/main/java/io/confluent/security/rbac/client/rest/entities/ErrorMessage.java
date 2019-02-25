// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Generic JSON error message.
 */
public class ErrorMessage {

  private int errorCode;
  private String message;

  public ErrorMessage(@JsonProperty("error_code") int errorCode,
                      @JsonProperty("message") String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  @JsonProperty("error_code")
  public int errorCode() {
    return errorCode;
  }

  @JsonProperty("error_code")
  public void errorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @JsonProperty
  public String message() {
    return message;
  }

  @JsonProperty
  public void message(String message) {
    this.message = message;
  }
}
