// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.utils.Optional;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.util.EnumMap;

/**
 * A cache of {@link TransformableSchema} for Kafka APIs which apply the multi-tenant
 * transformations needed to enforce tenant isolation. In particular, topic names, consumer groups,
 * and producer transactional ids are prefixed with the tenant name on request deserialization.
 * On response serialization, this prefix is stripped off.
 */
public class MultiTenantApis {

  private static final EnumMap<ApiKeys, TransformableType<TenantContext>[]> requestSchemas =
      new EnumMap<>(ApiKeys.class);
  private static final EnumMap<ApiKeys, TransformableType<TenantContext>[]> responseSchemas =
      new EnumMap<>(ApiKeys.class);

  static {
    for (ApiKeys api : ApiKeys.values()) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<TenantContext>[] apiRequestSchemas =
          new TransformableType[api.latestVersion() + 1];
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformableType<TenantContext>[] apiResponseSchemas =
          new TransformableType[api.latestVersion() + 1];
      TenantRequestSchemaSelector requestFieldSelector = new TenantRequestSchemaSelector(api);
      TenantResponseSchemaSelector responseSchemaSelector = new TenantResponseSchemaSelector(api);

      for (short version = api.oldestVersion(); version <= api.latestVersion(); version++) {
        apiRequestSchemas[version] = TransformableSchema.transformSchema(
            api.requestSchema(version), requestFieldSelector);
        apiResponseSchemas[version] = TransformableSchema.transformSchema(
            api.responseSchema(version), responseSchemaSelector);
      }

      requestSchemas.put(api, apiRequestSchemas);
      responseSchemas.put(api, apiResponseSchemas);
    }
  }

  /**
   * Check whether an API is supported. Internal broker and ACL APIs are not allowed through the
   * interceptor. There is no reason to expose the former and the latter requires finer-grained
   * authorization handling than is currently possible. In particular, we need to restrict the
   * resource types that the user is allowed to modify, but this cannot be done through an ACL
   * (the CreateAcl API is only authorized by Alter(Cluster)), and cannot be done through the
   * interceptor itself unless we're willing to fail the full request.
   */
  public static boolean isApiAllowed(ApiKeys api) {
    switch (api) {
      case PRODUCE:
      case FETCH:
      case LIST_OFFSETS:
      case METADATA:
      case OFFSET_COMMIT:
      case OFFSET_FETCH:
      case FIND_COORDINATOR:
      case JOIN_GROUP:
      case HEARTBEAT:
      case LEAVE_GROUP:
      case SYNC_GROUP:
      case DESCRIBE_GROUPS:
      case LIST_GROUPS:
      case DELETE_GROUPS:
      case SASL_HANDSHAKE:
      case API_VERSIONS:
      case CREATE_TOPICS:
      case DELETE_TOPICS:
      case DELETE_RECORDS:
      case INIT_PRODUCER_ID:
      case ADD_PARTITIONS_TO_TXN:
      case ADD_OFFSETS_TO_TXN:
      case END_TXN:
      case TXN_OFFSET_COMMIT:
      case DESCRIBE_CONFIGS:
      case ALTER_CONFIGS:
      case SASL_AUTHENTICATE:
        return true;

      case CREATE_PARTITIONS: // Disallowed until it supports custom policies
      case CONTROLLED_SHUTDOWN:
      case LEADER_AND_ISR:
      case UPDATE_METADATA:
      case STOP_REPLICA:
      case OFFSET_FOR_LEADER_EPOCH:
      case WRITE_TXN_MARKERS:
      case CREATE_ACLS:
      case DESCRIBE_ACLS:
      case DELETE_ACLS:
      case ALTER_REPLICA_LOG_DIRS:
      case DESCRIBE_LOG_DIRS:
      case CREATE_DELEGATION_TOKEN:
      case DESCRIBE_DELEGATION_TOKEN:
      case RENEW_DELEGATION_TOKEN:
      case EXPIRE_DELEGATION_TOKEN:
        return false;

      default:
        throw new IllegalArgumentException("Unexpected api key " + api);
    }
  }

  public static TransformableType<TenantContext> requestSchema(ApiKeys api, short version) {
    return requestSchemas.get(api)[version];
  }

  public static TransformableType<TenantContext> responseSchema(ApiKeys api, short version) {
    return responseSchemas.get(api)[version];
  }

  private static TenantContext.ValueType commonTransformableType(Field field) {
    if (field == CommonFields.TOPIC_NAME) {
      return TenantContext.ValueType.TOPIC;
    } else if (field == CommonFields.GROUP_ID) {
      return TenantContext.ValueType.GROUP;
    } else if (field == CommonFields.TRANSACTIONAL_ID
        || field == CommonFields.NULLABLE_TRANSACTIONAL_ID) {
      return TenantContext.ValueType.TRANSACTIONAL_ID;
    } else {
      return null;
    }
  }

  private static void ensureStringType(Type type) {
    if (type != Type.STRING && type != Type.NULLABLE_STRING) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static void ensureArrayType(Type type) {
    if (!(type instanceof ArrayOf)) {
      throw new IllegalArgumentException("Unexpected transform type " + type);
    }
  }

  private static class TenantRequestSchemaSelector
      implements TransformableSchema.FieldSelector<TenantContext> {
    private final ApiKeys api;

    public TenantRequestSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<TenantContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
        case DELETE_TOPICS:
          if (field != null && field.name.equals("topics")) {
            return Optional.<TransformableType<TenantContext>>some(
                new ArrayTenantTransformer(type, TenantContext.ValueType.TOPIC,
                    TenantTransform.ADD_PREFIX));
          }
          break;

        case DESCRIBE_GROUPS:
          if (field != null && field.name.equals("group_ids")) {
            return Optional.<TransformableType<TenantContext>>some(
                new ArrayTenantTransformer(type, TenantContext.ValueType.GROUP,
                    TenantTransform.ADD_PREFIX));
          }
          break;

        case DELETE_GROUPS:
          if (field != null && field.name.equals("groups")) {
            return Optional.<TransformableType<TenantContext>>some(
                new ArrayTenantTransformer(type, TenantContext.ValueType.GROUP,
                    TenantTransform.ADD_PREFIX));
          }
          break;

        case FIND_COORDINATOR:
          if (field != null && field.name.equals("coordinator_key")) {
            return Optional.<TransformableType<TenantContext>>some(
                new StringTenantTransformer(type, TenantContext.ValueType.GROUP,
                    TenantTransform.ADD_PREFIX));
          }
          break;

        case DESCRIBE_CONFIGS:
        case ALTER_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new ConfigResourceTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case CREATE_ACLS:
          if (field != null && field.name.equals("creations") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new AclResourceTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DELETE_ACLS:
          if (field != null && field.name.equals("filters") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new AclResourceTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        case DESCRIBE_ACLS:
          // The resource type and name are located in the root schema, which has no field
          if (field == null) {
            return Optional.<TransformableType<TenantContext>>some(
                new AclResourceTenantTransformer(type, TenantTransform.ADD_PREFIX));
          }
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.<TransformableType<TenantContext>>some(
            new StringTenantTransformer(type, valueType, TenantTransform.ADD_PREFIX));
      }

      return Optional.none();
    }
  }

  private static class TenantResponseSchemaSelector
      implements TransformableSchema.FieldSelector<TenantContext> {
    private final ApiKeys api;

    public TenantResponseSchemaSelector(ApiKeys api) {
      this.api = api;
    }

    @Override
    public Optional<TransformableType<TenantContext>> maybeAddTransformableType(
        Field field, Type type) {
      switch (api) {
        case METADATA:
          if (field != null && field.name.equals("cluster_id")) {
            // Unlike the usual paths, the cluster id actually needs the tenant prefix
            // added in the response to ensure that each tenant sees a different id.
            return Optional.<TransformableType<TenantContext>>some(
                new ClusterIdSubstitution(type));
          }
          break;

        case DESCRIBE_CONFIGS:
        case ALTER_CONFIGS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new ConfigResourceTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case DELETE_ACLS:
          if (field != null && field.name.equals("matching_acls") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new AclResourceTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        case DESCRIBE_ACLS:
          if (field != null && field.name.equals("resources") && type instanceof Schema) {
            return Optional.<TransformableType<TenantContext>>some(
                new AclResourceTenantTransformer(type, TenantTransform.REMOVE_PREFIX));
          }
          break;

        default:
          // fall through
      }

      TenantContext.ValueType valueType = commonTransformableType(field);
      if (valueType != null) {
        return Optional.<TransformableType<TenantContext>>some(
            new StringTenantTransformer(type, valueType, TenantTransform.REMOVE_PREFIX));
      }

      if (field == CommonFields.ERROR_MESSAGE) {
        return Optional.<TransformableType<TenantContext>>some(new ErrorMessageSanitizer(type));
      }

      return Optional.none();
    }
  }

  private static class ClusterIdSubstitution extends AbstractTransformableType<TenantContext> {

    public ClusterIdSubstitution(Type type) {
      super(type);
      ensureStringType(type);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      return ctx.principal.tenantMetadata().clusterId;
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      return type.sizeOf(ctx.principal.tenantMetadata().clusterId);
    }
  }

  private enum TenantTransform {
    ADD_PREFIX, REMOVE_PREFIX
  }

  private static class ErrorMessageSanitizer
      extends AbstractTransformableType<TenantContext> {
    ErrorMessageSanitizer(Type type) {
      super(type);
      ensureStringType(type);
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      if (value == null) {
        return null;
      }
      return ctx.removeAllTenantPrefixes((String) value);
    }

    @Override
    public int sizeOf(Object value, TenantContext ctx) {
      if (value == null) {
        return type.sizeOf(null);
      }
      return type.sizeOf(value) - ctx.sizeOfRemovedPrefixes((String) value);
    }
  }

  private abstract static class AbstractTenantTransformer
      extends AbstractTransformableType<TenantContext> {
    private final TenantTransform transform;

    AbstractTenantTransformer(Type type, TenantTransform transform) {
      super(type);
      this.transform = transform;
    }

    String transformString(String value, TenantContext ctx) {
      switch (transform) {
        case ADD_PREFIX:
          return ctx.addTenantPrefix(value);
        case REMOVE_PREFIX:
          return ctx.removeTenantPrefix(value);
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }

    int sizeDelta(TenantContext ctx) {
      switch (transform) {
        case ADD_PREFIX:
          return ctx.prefixSizeInBytes;
        case REMOVE_PREFIX:
          return -ctx.prefixSizeInBytes;
        default:
          throw new IllegalArgumentException("Unhandled transform type " + transform);
      }
    }
  }

  private static class StringTenantTransformer extends AbstractTenantTransformer {
    private final TenantContext.ValueType valueType;

    private StringTenantTransformer(Type fieldType, TenantContext.ValueType valueType,
                                    TenantTransform transform) {
      super(fieldType, transform);
      ensureStringType(fieldType);
      this.valueType = valueType;
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      return value == null ? null : transformString((String) value, ctx);
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      if (o == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(o) + sizeDelta(ctx);
      }
    }

  }

  private static class ArrayTenantTransformer extends AbstractTenantTransformer {
    private final TenantContext.ValueType valueType;

    private ArrayTenantTransformer(Type type, TenantContext.ValueType valueType,
                                   TenantTransform transform) {
      super(type, transform);
      ensureArrayType(type);
      ensureStringType(((ArrayOf) type).type());
      this.valueType = valueType;
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      Object[] array = (Object[]) value;
      if (array != null) {
        for (int i = 0; i < array.length; i++) {
          array[i] = transformString((String) array[i], ctx);
        }
      }
      return array;
    }

    @Override
    public int sizeOf(Object o, TenantContext ctx) {
      Object[] array = (Object[]) o;
      if (array == null) {
        return type.sizeOf(null);
      } else {
        return type.sizeOf(array) + array.length * sizeDelta(ctx);
      }
    }

  }

  private abstract static class ResourceTenantTransformer extends AbstractTenantTransformer {
    static final String RESOURCE_TYPE = "resource_type";
    static final String RESOURCE_NAME = "resource_name";

    private ResourceTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);

      if (!(type instanceof Schema)) {
        throw new IllegalArgumentException("Unexpected transform type " + type);
      }

      Schema schema = (Schema) type;
      if (schema.get(RESOURCE_NAME) == null) {
        throw new IllegalArgumentException("Expected type " + type + " to have "
            + RESOURCE_NAME + " field");
      }

      if (schema.get(RESOURCE_TYPE) == null) {
        throw new IllegalArgumentException("Expected type " + type + " to have "
            + RESOURCE_TYPE + " field");
      }
    }

    @Override
    public Object transform(Object value, TenantContext ctx) {
      Struct struct = (Struct) value;
      TenantContext.ValueType valueType = valueTypeOrNull(struct);
      if (valueType != null) {
        String name = struct.getString(RESOURCE_NAME);
        struct.set(RESOURCE_NAME, name == null ? null : transformString(name, ctx));
      }
      return struct;
    }

    @Override
    public int sizeOf(Object value, TenantContext ctx) {
      int size = type.sizeOf(value);
      Struct struct = (Struct) value;
      TenantContext.ValueType valueType = valueTypeOrNull((Struct) value);
      if (valueType != null) {
        String name = struct.getString(RESOURCE_NAME);
        if (name != null) {
          size += sizeDelta(ctx);
        }
      }
      return size;
    }

    abstract TenantContext.ValueType valueTypeOrNull(Struct struct);
  }

  private static class ConfigResourceTenantTransformer extends ResourceTenantTransformer {
    public ConfigResourceTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);
    }

    @Override
    TenantContext.ValueType valueTypeOrNull(Struct struct) {
      if (!struct.hasField(RESOURCE_TYPE)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      ConfigResource.Type resourceType = ConfigResource.Type.forId(struct.getByte(RESOURCE_TYPE));
      switch (resourceType) {
        case TOPIC:
          return TenantContext.ValueType.TOPIC;
        default:
          return null;
      }
    }
  }

  private static class AclResourceTenantTransformer extends ResourceTenantTransformer {
    public AclResourceTenantTransformer(Type type, TenantTransform transform) {
      super(type, transform);
    }

    @Override
    TenantContext.ValueType valueTypeOrNull(Struct struct) {
      if (!struct.hasField(RESOURCE_TYPE)) {
        throw new IllegalArgumentException("Unexpected transform type " + struct);
      }

      org.apache.kafka.common.resource.ResourceType resourceType =
          org.apache.kafka.common.resource.ResourceType.fromCode(struct.getByte(RESOURCE_TYPE));
      switch (resourceType) {
        case TOPIC:
          return TenantContext.ValueType.TOPIC;
        case GROUP:
          return TenantContext.ValueType.GROUP;
        case TRANSACTIONAL_ID:
          return TenantContext.ValueType.TRANSACTIONAL_ID;
        default:
          return null;
      }
    }
  }

}
