// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.external;

import java.util.Map;

public interface ExternalStoreListener<K, V> {

  void initialize(Map<K, V> initialValues);

  void update(K key, V value);

  void delete(K key);
}
