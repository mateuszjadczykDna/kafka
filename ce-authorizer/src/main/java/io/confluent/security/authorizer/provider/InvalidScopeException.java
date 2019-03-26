// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import org.apache.kafka.common.errors.ApiException;

public class InvalidScopeException extends ApiException {

  public InvalidScopeException(String message) {
    super(message);
  }
}
