// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import java.nio.ByteBuffer;

public interface TransformableType<T extends TransformContext> {

  void write(ByteBuffer buffer, Object o, T ctx);

  Object read(ByteBuffer buffer, T ctx);

  int sizeOf(Object o, T ctx);

}
