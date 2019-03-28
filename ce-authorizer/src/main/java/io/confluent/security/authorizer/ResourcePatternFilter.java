// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.common.resource.PatternType;

/**
 * Represents a resource pattern filter that can be used to match resources while
 * describing or deleting access rules.
 */
public class ResourcePatternFilter {

  private final String name;
  private final ResourceType resourceType;
  private final PatternType patternType;

  public ResourcePatternFilter(@JsonProperty("resourceType") ResourceType resourceType,
                               @JsonProperty("name") String name,
                               @JsonProperty("patternType") PatternType patternType) {
    this.name = name;
    this.resourceType = resourceType;
    this.patternType = patternType;
  }

  @JsonProperty
  public String name() {
    return name;
  }

  @JsonProperty
  public ResourceType resourceType() {
    return resourceType;
  }

  @JsonProperty
  public PatternType patternType() {
    return patternType;
  }

  public boolean matches(ResourcePattern resource) {
    if (resourceType() != null && !resourceType().equals(resource.resourceType()))
      return false;

    // For cross-component resources, we check resource type separately. We want to use common
    // logic for matching the rest of the parameters to ensure pattern types are handled consistently.
    org.apache.kafka.common.resource.ResourceType unusedResourceType = org.apache.kafka.common.resource.ResourceType.TOPIC;

    org.apache.kafka.common.resource.ResourcePatternFilter filter =
        new org.apache.kafka.common.resource.ResourcePatternFilter(unusedResourceType, name(), patternType());
    org.apache.kafka.common.resource.ResourcePattern pattern =
        new org.apache.kafka.common.resource.ResourcePattern(unusedResourceType, resource.name(), resource.patternType());
    return filter.matches(pattern);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResourcePatternFilter)) {
      return false;
    }

    ResourcePatternFilter that = (ResourcePatternFilter) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.resourceType, that.resourceType) &&
        Objects.equals(this.patternType, that.patternType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, resourceType, patternType);
  }

  @Override
  public String toString() {
    return String.format("%s:%s:%s", resourceType, patternType, name); // Same format as AK
  }
}
