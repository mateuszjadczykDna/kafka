// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Role definition including access policy.
 */
public class Role {

  private final String name;
  private final AccessPolicy accessPolicy;

  @JsonCreator
  public Role(@JsonProperty("name") String name, @JsonProperty("policy") AccessPolicy accessPolicy) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Role name must be non-empty");
    this.name = name;
    this.accessPolicy = Objects.requireNonNull(accessPolicy, "access policy must not be null");
  }

  @JsonProperty
  public String name() {
    return name;
  }

  @JsonProperty
  public AccessPolicy accessPolicy() {
    return accessPolicy;
  }

  @JsonIgnore
  public boolean hasResourceScope() {
    return AccessPolicy.RESOURCE_SCOPE.equals(accessPolicy.scopeType());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Role)) {
      return false;
    }

    Role that = (Role) o;
    return Objects.equals(name, that.name) && Objects.equals(accessPolicy, that.accessPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, accessPolicy);
  }

  @Override
  public String toString() {
    return name;
  }
}
