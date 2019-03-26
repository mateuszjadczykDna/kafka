// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.Resource;
import io.confluent.security.authorizer.ResourceType;
import java.util.HashMap;
import java.util.Map;
import kafka.security.auth.Acl;
import kafka.security.auth.Operation$;
import kafka.security.auth.PermissionType;
import kafka.security.auth.PermissionType$;
import kafka.security.auth.ResourceType$;
import scala.collection.JavaConversions;


/**
 * Maps Kafka ACL and related classes to Confluent cross-component authorization classes.
 */
public class AclMapper {

  private static final Map<kafka.security.auth.ResourceType, ResourceType> RESOURCE_TYPES;
  private static final Map<kafka.security.auth.Operation, Operation> OPERATIONS;
  private static final Map<PermissionType, io.confluent.security.authorizer.PermissionType> PERMISSION_TYPES;
  private static final Map<ResourceType, kafka.security.auth.ResourceType> KAFKA_RESOURCE_TYPES;
  private static final Map<Operation, kafka.security.auth.Operation> KAFKA_OPERATIONS;
  private static final Map<io.confluent.security.authorizer.PermissionType, PermissionType> KAFKA_PERMISSION_TYPES;

  static {
    KAFKA_RESOURCE_TYPES = new HashMap<>();
    RESOURCE_TYPES = new HashMap<>();
    JavaConversions.seqAsJavaList(ResourceType$.MODULE$.values()).forEach(kafkaResourceType -> {
      ResourceType resourceType = new ResourceType(
          kafkaResourceType.name());
      KAFKA_RESOURCE_TYPES.put(resourceType, kafkaResourceType);
      RESOURCE_TYPES.put(kafkaResourceType, resourceType);
    });

    KAFKA_OPERATIONS = new HashMap<>();
    OPERATIONS = new HashMap<>();
    JavaConversions.seqAsJavaList(Operation$.MODULE$.values()).forEach(kafkaOperation -> {
      Operation operation = new Operation(kafkaOperation.name());
      KAFKA_OPERATIONS.put(operation, kafkaOperation);
      OPERATIONS.put(kafkaOperation, operation);
    });

    KAFKA_PERMISSION_TYPES = new HashMap<>();
    PERMISSION_TYPES = new HashMap<>();
    for (io.confluent.security.authorizer.PermissionType permissionType : io.confluent.security.authorizer.PermissionType
        .values()) {
      PermissionType kafkaPermissionType = PermissionType$.MODULE$
          .fromString(permissionType.name());
      KAFKA_PERMISSION_TYPES.put(permissionType, kafkaPermissionType);
      PERMISSION_TYPES.put(kafkaPermissionType, permissionType);
    }
  }

  public static kafka.security.auth.ResourceType kafkaResourceType(ResourceType resourceType) {
    return mapValueOrFail(KAFKA_RESOURCE_TYPES, resourceType);
  }

  public static kafka.security.auth.Operation kafkaOperation(Operation operation) {
    return mapValueOrFail(KAFKA_OPERATIONS, operation);
  }

  public static PermissionType kafkaPermissionType(
      io.confluent.security.authorizer.PermissionType permissionType) {
    return mapValueOrFail(KAFKA_PERMISSION_TYPES, permissionType);
  }

  public static ResourceType resourceType(kafka.security.auth.ResourceType resourceType) {
    return mapValueOrFail(RESOURCE_TYPES, resourceType);
  }

  public static Operation operation(kafka.security.auth.Operation operation) {
    return mapValueOrFail(OPERATIONS, operation);
  }

  public static io.confluent.security.authorizer.PermissionType permissionType(PermissionType permissionType) {
    return mapValueOrFail(PERMISSION_TYPES, permissionType);
  }

  public static Resource resource(kafka.security.auth.Resource resource) {
    return new Resource(resourceType(resource.resourceType()), resource.name(), resource.patternType());
  }

  private static <K, V> V mapValueOrFail(Map<K, V> map, K key) {
    V value = map.get(key);
    if (value == null)
      throw new IllegalArgumentException(String.format("Value is null for %s", key));
    else
      return value;
  }

  public static AccessRule accessRule(Acl acl) {
    return new AccessRule(acl.principal(),
        permissionType(acl.permissionType()),
        acl.host(),
        operation(acl.operation()),
        acl.toString());
  }
}
