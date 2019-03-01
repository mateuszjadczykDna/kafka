// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import org.apache.kafka.common.errors.ApiException;

public class InvalidRoleAssignmentException extends ApiException {

  public InvalidRoleAssignmentException(String message) {
    super(message);
  }

  public InvalidRoleAssignmentException(String message, Throwable cause) {
    super(message, cause);
  }
}
