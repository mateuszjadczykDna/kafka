// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Role assignment that assigns a role to a user or group principal at a specified scope.
 * The assignment may be scoped to specific resources by specifying a collection of resources.
 */
public class RoleAssignment {

  private final KafkaPrincipal principal;
  private final Collection<RbacResource> resources;
  private final String role;
  private final String scope;

  @JsonCreator
  public RoleAssignment(@JsonProperty("principal") KafkaPrincipal principal,
                        @JsonProperty("role") String role,
                        @JsonProperty("scope") String scope,
                        @JsonProperty("resources") Collection<RbacResource> resources) {
    this.principal = principal;
    this.role = role;
    this.scope = scope;
    this.resources = resources == null ? Collections.emptySet() :
        Collections.unmodifiableList(new ArrayList<>(resources));
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
  public Collection<RbacResource> resources() {
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
