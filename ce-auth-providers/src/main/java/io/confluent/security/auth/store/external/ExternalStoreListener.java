// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.external;

import java.util.Map;

/**
 * Listener for entries from an external store, for example an LDAP server.
 * This is implemented by the auth metadata writer. The master writer processes
 * entries from the external store using this listener interface.
 *
 * @param <K> Key type for entries in the external store
 * @param <V> Value type for entries in the external store
 */
public interface ExternalStoreListener<K, V> {

  /**
   * Invoked when existing entries from the external store are processed.
   *
   * @param initialValues Initial values from the store
   */
  void initialize(Map<K, V> initialValues);

  /**
   * Invoked when a refresh of the external store finds a new entry or an
   * updated entry.
   *
   * @param key Key to update
   * @param value New value
   */
  void update(K key, V value);

  /**
   * Invoked when a refresh of the external store indicates that an entry
   * was deleted.
   *
   * @param key Key of deleted entry
   */
  void delete(K key);

  /**
   * Indicates that the connection to the external store has failed. This is
   * invoked when the configured retry timeout for the store has been reached.
   *
   * @param errorMessage Error message that will be added to the auth topic
   */
  void fail(String errorMessage);

  /**
   * Indicates that external store is functioning after an earlier failure.
   */
  void resetFailure();
}
