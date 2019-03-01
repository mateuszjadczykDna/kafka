// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store;

import java.util.Map;

/**
 * Key value store implemented by in-memory cache backed with data from a potentially partitioned
 * source like a Kafka topic. Status must be tracked separately for each partition by the implementation
 * of this store.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KeyValueStore<K, V> {

  /**
   * Returns the current value associated with key if it exists or null otherwise.
   */
  V get(K key);

  /**
   * Updates the entry for key with the specified value. Value must not be null.
   * Data records as well as remote status and error records are processed using this method.
   *
   * @return old value if key was present in the store, null otherwise
   */
  V put(K key, V value);

  /**
   * Removes the entry corresponding to the key if it is present in the store.
   *
   * @return old value if key was present in the store, null otherwise
   */
  V remove(K key);

  Map<? extends K, ? extends V> map(String entryType);

  /**
   * Sets local error for the store with the specified error message.
   * This is invoked if the reader encountered a fatal error.
   */
  void fail(int partition, String errorMessage);

  /**
   * Status of the metadata store for the specified partition
   */
  MetadataStoreStatus status(int partition);
}
