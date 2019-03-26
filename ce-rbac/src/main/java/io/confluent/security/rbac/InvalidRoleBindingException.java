// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import org.apache.kafka.common.errors.ApiException;

public class InvalidRoleBindingException extends ApiException {

  public InvalidRoleBindingException(String message) {
    super(message);
  }

  public InvalidRoleBindingException(String message, Throwable cause) {
    super(message, cause);
  }
}
