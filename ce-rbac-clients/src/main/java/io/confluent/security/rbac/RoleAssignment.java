// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.security.authorizer.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Role assignment that assigns a role to a user or group principal at a specified scope.
 * The assignment may be scoped to specific resources by specifying a collection of resources.
 */
public class RoleAssignment {

  private final KafkaPrincipal principal;
  private final Collection<Resource> resources;
  private final String role;
  private final String scope;

  @JsonCreator
  public RoleAssignment(@JsonProperty("principal") KafkaPrincipal principal,
                        @JsonProperty("role") String role,
                        @JsonProperty("scope") String scope,
                        @JsonProperty("resources") Collection<Resource> resources) {
    this.principal = Objects.requireNonNull(principal, "principal must not be null");
    if (role == null || role.isEmpty())
      throw new IllegalArgumentException("Role must be non-empty for role assignment");
    this.role = role;
    if (scope == null || scope.isEmpty())
      throw new IllegalArgumentException("Scope must be non-empty for role assignment");
    this.scope = scope;
    this.resources = resources == null ? Collections.emptySet() :
        Collections.unmodifiableSet(new HashSet<>(resources));
  }

  @JsonProperty
  public KafkaPrincipal principal() {
    return principal;
  }

  @JsonProperty
  public String role() {
    return role;
  }

  @JsonProperty
  public String scope() {
    return scope;
  }

  @JsonProperty
  public Collection<Resource> resources() {
    return resources;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleAssignment)) {
      return false;
    }

    RoleAssignment that = (RoleAssignment) o;
    return Objects.equals(principal, that.principal) &&
        Objects.equals(role, that.role) &&
        Objects.equals(scope, that.scope) &&
        Objects.equals(resources, that.resources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, role, scope, resources);
  }

  @Override
  public String toString() {
    return "RoleAssignment(" +
        "principal=" + principal +
        ", role='" + role + '\'' +
        ", scope='" + scope + '\'' +
        ", resources=" + resources +
        ')';
  }
}
