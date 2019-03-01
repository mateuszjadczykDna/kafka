// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class UserKey extends AuthKey {

  private final KafkaPrincipal principal;

  @JsonCreator
  public UserKey(@JsonProperty("principal") KafkaPrincipal principal) {
    this.principal = principal;
  }

  @JsonProperty
  public KafkaPrincipal principal() {
    return principal;
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
    if (!(o instanceof UserKey)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    UserKey that = (UserKey) o;

    return Objects.equals(principal, that.principal);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(principal);
  }

  @Override
  public String toString() {
    return principal.toString();
  }
}
