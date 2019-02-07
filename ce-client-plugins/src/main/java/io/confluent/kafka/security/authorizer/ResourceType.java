// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import java.util.Objects;

public class ResourceType {

  public static final ResourceType ALL = new ResourceType("All");
  public static final ResourceType CLUSTER = new ResourceType("Cluster");

  private final String name;

  public ResourceType(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResourceType)) {
      return false;
    }

    ResourceType that = (ResourceType) o;
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
