// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import java.util.Objects;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;

/**
 * Represents an authorizable resource with resource types that are not pre-defined, enabling this
 * class to be used for authorization in different components.
 */
public class Resource implements Comparable<Resource> {

  public static final Resource ALL =
      new Resource(ResourceType.ALL, ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL);
  public static final Resource CLUSTER =
      new Resource(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);

  private final String name;
  private final ResourceType resourceType;
  private final PatternType patternType;

  public Resource(ResourceType resourceType, String name, PatternType patternType) {
    this.name = name;
    this.resourceType = resourceType;
    this.patternType = patternType;
  }

  public String name() {
    return name;
  }

  public ResourceType resourceType() {
    return resourceType;
  }

  public PatternType patternType() {
    return patternType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Resource)) {
      return false;
    }

    Resource that = (Resource) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.resourceType, that.resourceType) &&
        Objects.equals(this.patternType, that.patternType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, resourceType, patternType);
  }

  @Override
  public int compareTo(Resource other) {
    int result = resourceType().name().compareTo(other.resourceType().name());
    if (result == 0) {
      result = patternType().compareTo(other.patternType());
      if (result == 0) {
        result = other.name().compareTo(name()); // reverse ordering for name
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s:%s:%s", resourceType, patternType, name); // Same format as AK
  }

  public static Resource all(ResourceType resourceType) {
    return resourceType.equals(ResourceType.CLUSTER) ? CLUSTER :
        new Resource(resourceType, ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL);
  }
}
