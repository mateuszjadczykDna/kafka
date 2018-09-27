// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;

public abstract class AbstractTransformableType<T extends TransformContext>
    implements TransformableType<T> {

  protected final Type type;

  public AbstractTransformableType(Type type) {
    this.type = type;
  }

  public void write(ByteBuffer buffer, Object o, T ctx) {
    type.write(buffer, transform(o, ctx));
  }

  public Object read(ByteBuffer buffer, T ctx) {
    return transform(type.read(buffer), ctx);
  }

  public int sizeOf(Object o, T ctx) {
    return type.sizeOf(transform(o, ctx));
  }

  public abstract Object transform(Object value, T ctx);
}
