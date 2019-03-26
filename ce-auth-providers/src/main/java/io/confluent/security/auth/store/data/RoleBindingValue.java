// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RoleBindingValue extends AuthValue {

  private final Set<Resource> resources;

  @JsonCreator
  public RoleBindingValue(@JsonProperty("resources") Collection<Resource> resources) {
    this.resources = resources == null ? Collections.emptySet() : new HashSet<>(resources);
  }

  @JsonProperty
  public Collection<Resource> resources() {
    return resources;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.ROLE_BINDING;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleBindingValue)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RoleBindingValue that = (RoleBindingValue) o;

    return Objects.equals(resources, that.resources);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(resources);
  }

  @Override
  public String toString() {
    return "RoleBindingValue(" +
        "resources=" + resources +
        ')';
  }
}
