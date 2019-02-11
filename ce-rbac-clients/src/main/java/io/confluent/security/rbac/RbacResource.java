// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.ResourceType;
import org.apache.kafka.common.resource.PatternType;

/**
 * Resource with a JSON representation to store in role assignments.
 */
public class RbacResource extends Resource {

  @JsonCreator
  public RbacResource(@JsonProperty("type") String type,
                      @JsonProperty("name") String name,
                      @JsonProperty("patternType") PatternType patternType) {
    super(new ResourceType(type), name, patternType);
  }

  @JsonProperty
  public String name() {
    return super.name();
  }

  @JsonProperty
  public String type() {
    return super.resourceType().name();
  }

  @JsonProperty
  public PatternType patternType() {
    return super.patternType();
  }

}
