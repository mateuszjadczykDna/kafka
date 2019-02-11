// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * User metadata including groups which may be obtained from LDAP.
 */
public class UserMetadata {

  private final Set<KafkaPrincipal> groups;

  @JsonCreator
  public UserMetadata(@JsonProperty("groups") Set<KafkaPrincipal> groups) {
    this.groups = groups == null ? Collections.emptySet() : Collections.unmodifiableSet(groups);
  }

  @JsonProperty
  public Set<KafkaPrincipal> groups() {
    return groups;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UserMetadata)) {
      return false;
    }

    UserMetadata that = (UserMetadata) o;

    return Objects.equals(this.groups, that.groups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groups);
  }
}
