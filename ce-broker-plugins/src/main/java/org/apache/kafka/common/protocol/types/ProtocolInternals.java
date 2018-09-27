// (Copyright) [2017 - 2017] Confluent, Inc.

package org.apache.kafka.common.protocol.types;

// Expose some stuff from org.apache.kafka.common.protocol.types that wasn't meant to be exposed
public class ProtocolInternals {

  public static Struct newStruct(Schema schema, Object[] values) {
    return new Struct(schema, values);
  }

}
