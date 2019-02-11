// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import org.apache.kafka.common.errors.ApiException;

public class InvalidRoleDefinitionException extends ApiException {

  public InvalidRoleDefinitionException(String message) {
    super(message);
  }

  public InvalidRoleDefinitionException(String message, Throwable cause) {
    super(message, cause);
  }
}
