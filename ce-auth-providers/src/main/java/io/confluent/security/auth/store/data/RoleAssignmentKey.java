// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class RoleAssignmentKey extends AuthKey {

  private final KafkaPrincipal principal;
  private final String role;
  private final String scope;

  @JsonCreator
  public RoleAssignmentKey(@JsonProperty("principal") KafkaPrincipal principal,
                           @JsonProperty("role") String role,
                           @JsonProperty("scope") String scope) {
    this.principal = principal;
    this.role = role;
    this.scope = scope;
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

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.ROLE_ASSIGNMENT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleAssignmentKey)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RoleAssignmentKey that = (RoleAssignmentKey) o;
    return Objects.equals(principal, that.principal) &&
        Objects.equals(role, that.role) &&
        Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, role, scope);
  }

  @Override
  public String toString() {
    return "RoleAssignmentKey{" +
        "principal=" + principal +
        ", role='" + role + '\'' +
        ", scope='" + scope + '\'' +
        '}';
  }
}
