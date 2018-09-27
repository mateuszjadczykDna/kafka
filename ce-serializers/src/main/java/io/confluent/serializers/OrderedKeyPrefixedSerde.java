/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.serializers;

import org.apache.kafka.common.utils.Bytes;

public interface OrderedKeyPrefixedSerde<E extends Enum, T> extends OrderedKeyUberSerde<T>  {
  /**
   * Returns the prefix used for serializing keys
   */
  E prefix();

  /**
   * Extracts the prefix from the byte array of the key
   */
  E extractPrefix(Bytes key);

  OrderedKeyPrefixedSerde<E, T> prefixKeySerde(int numFields);
}
