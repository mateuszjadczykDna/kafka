// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.multitenant.schema;

import io.confluent.kafka.multitenant.utils.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.RequestInternals;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransformableSchemaTest {

  @Test
  public void testReadAndTransformPrimitiveArray() throws IOException {
    short describeGroupVersion = ApiKeys.DESCRIBE_GROUPS.latestVersion();
    Schema describeGroupSchema = ApiKeys.DESCRIBE_GROUPS.requestSchema(describeGroupVersion);

    TransformableType<TransformContext> transformSchema = TransformableSchema.transformSchema(describeGroupSchema,
        new TransformableSchema.FieldSelector<TransformContext>() {
          @Override
          public Optional<TransformableType<TransformContext>> maybeAddTransformableType(Field field, final Type type) {
            if (field != null && field.name.equals("groups")) {
              return Optional.<TransformableType<TransformContext>>some(new AbstractTransformableType<TransformContext>(type) {
                @Override
                public Object transform(Object value, TransformContext ctx) {
                  Object[] array = (Object[]) value;
                  Object[] copy = new Object[array.length];
                  for (int i = 0; i < array.length; i++) {
                    copy[i] = "foo." + array[i];
                  }
                  return copy;
                }
              });
            }
            return Optional.none();
          }
        });

    DescribeGroupsRequestData describeGroupsRequestData = new DescribeGroupsRequestData();
    describeGroupsRequestData.setGroups(Arrays.asList("a", "b"));
    DescribeGroupsRequest request = new DescribeGroupsRequest.Builder(describeGroupsRequestData).build(describeGroupVersion);
    Struct originalStruct = RequestInternals.toStruct(request);
    ByteBuffer serializedRequest = toByteBuffer(originalStruct);

    Struct transformedStruct = (Struct) transformSchema.read(serializedRequest, null);
    DescribeGroupsRequest transformedRequest = new DescribeGroupsRequest(transformedStruct, describeGroupVersion);
    assertEquals(Arrays.asList("foo.a", "foo.b"), transformedRequest.data().groups());
    assertEquals(transformedStruct.sizeOf(), transformSchema.sizeOf(originalStruct, null));
  }

  @Test
  public void testWriteAndTransformPrimitiveArray() throws IOException {
    short describeGroupVersion = ApiKeys.DESCRIBE_GROUPS.latestVersion();
    Schema describeGroupSchema = ApiKeys.DESCRIBE_GROUPS.requestSchema(describeGroupVersion);

    TransformableType<TransformContext> transformSchema = TransformableSchema.transformSchema(describeGroupSchema,
        new TransformableSchema.FieldSelector<TransformContext>() {
          @Override
          public Optional<TransformableType<TransformContext>> maybeAddTransformableType(Field field, final Type type) {
            if (field != null && field.name.equals("groups")) {
              return Optional.<TransformableType<TransformContext>>some(new AbstractTransformableType<TransformContext>(type) {
                @Override
                public Object transform(Object value, TransformContext ctx) {
                  Object[] array = (Object[]) value;
                  Object[] copy = new Object[array.length];
                  for (int i = 0; i < array.length; i++) {
                    copy[i] = ((String) array[i]).substring(4);
                  }
                  return copy;
                }
              });
            }
            return Optional.none();
          }
        });

    DescribeGroupsRequestData describeGroupsRequestData = new DescribeGroupsRequestData();
    describeGroupsRequestData.setGroups(Arrays.asList("foo.a", "foo.b"));
    DescribeGroupsRequest request = new DescribeGroupsRequest.Builder(describeGroupsRequestData).build(describeGroupVersion);
    Struct originalStruct = RequestInternals.toStruct(request);

    int transformedSize = transformSchema.sizeOf(originalStruct, null);
    ByteBuffer buf = ByteBuffer.allocate(transformedSize);
    transformSchema.write(buf, originalStruct, null);
    buf.flip();
    Struct transformedStruct = describeGroupSchema.read(buf);
    DescribeGroupsRequest transformedRequest = new DescribeGroupsRequest(transformedStruct, describeGroupVersion);
    assertEquals(Arrays.asList("a", "b"), transformedRequest.data().groups());
    assertEquals(transformedSize, transformedStruct.sizeOf());
  }

  @Test
  public void testReadAndTransform() throws IOException {
    short fetchVersion = ApiKeys.FETCH.latestVersion();
    Schema fetchSchema = ApiKeys.FETCH.requestSchema(fetchVersion);

    TransformableType<TransformContext> transformSchema = TransformableSchema.transformSchema(fetchSchema,
        new TransformableSchema.FieldSelector<TransformContext>() {
          @Override
          public Optional<TransformableType<TransformContext>> maybeAddTransformableType(Field field, final Type type) {
            if (field == CommonFields.TOPIC_NAME) {
              return Optional.<TransformableType<TransformContext>>some(new AbstractTransformableType<TransformContext>(type) {
                @Override
                public Object transform(Object value, TransformContext ctx) {
                  return "foo." + value;
                }
              });
            }
            return Optional.none();
          }
        });

    LinkedHashMap<TopicPartition, FetchRequest.PartitionData> partitions = new LinkedHashMap<>();
    partitions.put(new TopicPartition("bar", 0), new FetchRequest.PartitionData(10L, -1, 1024 * 1024, java.util.Optional.empty()));
    FetchRequest.Builder bldr = FetchRequest.Builder.forConsumer(0, 1024, partitions);
    Struct originalStruct = RequestInternals.toStruct(bldr.build(fetchVersion));
    ByteBuffer serializedRequest = toByteBuffer(originalStruct);

    Struct transformedStruct = (Struct) transformSchema.read(serializedRequest, null);
    FetchRequest transformedRequest = new FetchRequest(transformedStruct, fetchVersion);
    Map<TopicPartition, FetchRequest.PartitionData> transformedPartitions = transformedRequest.fetchData();
    assertEquals(1, transformedPartitions.size());
    assertTrue(transformedPartitions.containsKey(new TopicPartition("foo.bar", 0)));
    assertEquals(transformedStruct.sizeOf(), transformSchema.sizeOf(originalStruct, null));
  }

  @Test
  public void testWriteAndTransform() throws Exception {
    short fetchVersion = ApiKeys.FETCH.latestVersion();
    Schema fetchSchema = ApiKeys.FETCH.requestSchema(fetchVersion);

    TransformableType<TransformContext> transformSchema = TransformableSchema.transformSchema(fetchSchema,
        new TransformableSchema.FieldSelector<TransformContext>() {
          @Override
          public Optional<TransformableType<TransformContext>> maybeAddTransformableType(Field field, final Type type) {
            if (field == CommonFields.TOPIC_NAME) {
              return Optional.<TransformableType<TransformContext>>some(new AbstractTransformableType<TransformContext>(type) {
                @Override
                public Object transform(Object value, TransformContext ctx) {
                  return ((String) value).substring(4);
                }
              });
            }
            return Optional.none();
          }
        });

    LinkedHashMap<TopicPartition, FetchRequest.PartitionData> partitions = new LinkedHashMap<>();
    partitions.put(new TopicPartition("foo.bar", 0), new FetchRequest.PartitionData(10L, -1, 1024 * 1024, java.util.Optional.empty()));
    FetchRequest.Builder bldr = FetchRequest.Builder.forConsumer(0, 1024, partitions);
    Struct originalStruct = RequestInternals.toStruct(bldr.build(fetchVersion));

    int transformedSize = transformSchema.sizeOf(originalStruct, null);
    ByteBuffer buf = ByteBuffer.allocate(transformedSize);
    transformSchema.write(buf, originalStruct, null);
    buf.flip();
    Struct transformedStruct = fetchSchema.read(buf);
    FetchRequest fetchRequest = new FetchRequest(transformedStruct, fetchVersion);
    Map<TopicPartition, FetchRequest.PartitionData> transformedPartitions = fetchRequest.fetchData();
    assertEquals(1, transformedPartitions.size());
    assertTrue(transformedPartitions.containsKey(new TopicPartition("bar", 0)));
    assertEquals(transformedSize, transformedStruct.sizeOf());
  }

  private static ByteBuffer toByteBuffer(Struct struct) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
    struct.writeTo(buffer);
    buffer.flip();
    return buffer;
  }

}
