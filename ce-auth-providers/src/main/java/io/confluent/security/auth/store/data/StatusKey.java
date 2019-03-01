// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class StatusKey extends AuthKey {

  private final int partition;

  @JsonCreator
  public StatusKey(@JsonProperty("partition") int partition) {
    this.partition = partition;
  }

  @JsonProperty
  public int partition() {
    return partition;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.STATUS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StatusKey)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    StatusKey that = (StatusKey) o;
    return this.partition == that.partition;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(partition);
  }

  @Override
  public String toString() {
    return "StatusKey(" +
        "partition=" + partition +
        ')';
  }
}
