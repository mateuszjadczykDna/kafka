// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import java.util.Objects;
import org.apache.kafka.common.resource.PatternType;

/**
 * Represents an authorizable action, which is an operation performed on a resource.
 */
public class Action {

  private final String scope;
  private final ResourceType resourceType;
  private final String resourceName;
  private final Operation operation;

  public Action(String scope,
                ResourceType resourceType,
                String resourceName,
                Operation operation) {
    this.scope = scope;
    this.resourceType = resourceType;
    this.resourceName = resourceName;
    this.operation = operation;
  }

  public String scope() {
    return scope;
  }

  public ResourceType resourceType() {
    return resourceType;
  }

  public String resourceName() {
    return resourceName;
  }

  public Operation operation() {
    return operation;
  }

  public Resource resource() {
    return new Resource(resourceType, resourceName, PatternType.LITERAL);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Action)) {
      return false;
    }

    Action that = (Action) o;
    return Objects.equals(this.scope, that.scope) &&
        Objects.equals(this.resourceType, that.resourceType) &&
        Objects.equals(this.resourceName, that.resourceName) &&
        Objects.equals(this.operation, that.operation);

  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, resourceType, resourceName, operation);
  }

  @Override
  public String toString() {
    return "Action(" +
        "scope='" + scope + '\'' +
        ", resourceType='" + resourceType + '\'' +
        ", resourceName='" + resourceName + '\'' +
        ", operation='" + operation + '\'' +
        ')';
  }
}
