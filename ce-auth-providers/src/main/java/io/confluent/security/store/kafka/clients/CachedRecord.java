// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

public class CachedRecord<K, V> {
  private final K key;
  private final V value;
  private final int generationIdDuringRead;

  CachedRecord(K key, V value, int generationIdDuringRead) {
    this.key = key;
    this.value = value;
    this.generationIdDuringRead = generationIdDuringRead;
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  public int generationIdDuringRead() {
    return generationIdDuringRead;
  }
}
