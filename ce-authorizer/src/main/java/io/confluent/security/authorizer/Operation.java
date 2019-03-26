// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Objects;

/**
 * Represents an authorizable operation, e.g. Read, Write. This includes all Kafka operations
 * and additional operations may be added dynamically. Authorizers don't check the validity of
 * operations against resource types, authorization succeeds if an access rule matches.
 */
public class Operation {
  public static final Operation ALL = new Operation("All");

  private final String name;

  public Operation(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  public boolean matches(Operation resourceOperation) {
    return this.equals(ALL) || this.equals(resourceOperation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Operation)) {
      return false;
    }

    Operation that = (Operation) o;
    return Objects.equals(this.name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
