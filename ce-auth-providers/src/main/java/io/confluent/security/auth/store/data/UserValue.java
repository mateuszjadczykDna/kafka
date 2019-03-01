// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class UserValue extends AuthValue {

  private final Set<KafkaPrincipal> groups;

  @JsonCreator
  public UserValue(@JsonProperty("groups") Collection<KafkaPrincipal> groups) {
    this.groups = groups == null ? Collections.emptySet() : new HashSet<>(groups);
  }

  @JsonProperty
  public Set<KafkaPrincipal> groups() {
    return groups;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.USER;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserValue)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    UserValue that = (UserValue) o;

    return Objects.equals(this.groups, that.groups);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(groups);
  }

  @Override
  public String toString() {
    return "UserValue(" +
        ", groups=" + groups +
        ')';
  }
}
