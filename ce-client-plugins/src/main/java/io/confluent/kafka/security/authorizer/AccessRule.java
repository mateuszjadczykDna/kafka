// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Encapsulates an access rule which may be derived from an ACL or RBAC policy.
 * Operations and resource types are extensible to enable this to be used for
 * authorization in different components.
 */
public class AccessRule {

  public static final String ALL_HOSTS = "*";
  public static final KafkaPrincipal WILDCARD_USER_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
  public static final String GROUP_PRINCIPAL_TYPE = "Group";
  public static final KafkaPrincipal WILDCARD_GROUP_PRINCIPAL = new KafkaPrincipal(GROUP_PRINCIPAL_TYPE, "*");

  private final KafkaPrincipal principal;
  private final PermissionType permissionType;
  private final String host;
  private final Operation operation;
  private final String sourceDescription;

  public AccessRule(KafkaPrincipal principal,
      PermissionType permissionType,
      String host,
      Operation operation,
      String sourceDescription) {
    this.principal = principal;
    this.permissionType = permissionType;
    this.operation = operation;
    this.host = host;
    this.sourceDescription = sourceDescription;
  }

  public KafkaPrincipal principal() {
    return principal;
  }

  public PermissionType permissionType() {
    return permissionType;
  }

  public String host() {
    return host;
  }

  public Operation operation() {
    return operation;
  }

  public String sourceDescription() {
    return sourceDescription;
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s from host %s (source: %s)",
        principal, permissionType, operation, host, sourceDescription);
  }
}
