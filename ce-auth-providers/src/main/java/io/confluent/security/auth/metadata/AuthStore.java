// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import java.io.Closeable;
import org.apache.kafka.common.Configurable;

/**
 * Store containing authorization and authentication metadata. This is backed by
 * a Kafka metadata topic.
 */
public interface AuthStore extends Configurable, Closeable {

  /**
   * Initializes this store, initializes its cache and then returns. Note that broker
   * does not start any external listeners until this method returns, so this method should
   * return immediately after initialization is complete. The cache must be initialized to
   * contain existing LDAP and RBAC metadata to avoid unexpected authorization exceptions
   * during broker start up. In a single-cluster set up, this store must be configured to use
   * the inter-broker listener to read the auth topic since external listeners are not started
   * until after the cache is populated.
   */
  void start();

  /**
   * Returns the cache that stores all data read from the auth topic.
   *
   * @return cache used for authentication and/or authorization
   */
  AuthCache authCache();

  /**
   * Adds a listener that is notified of all changes to auth metadata.
   *
   * @param listener Listener instance
   * @return Returns true if the listener was added and false if the listener was already present earlier.
   */
  boolean addListener(AuthListener listener);

  /**
   * Removes a previously registered listener.
   *
   * @param listener Listener instance
   * @return true if the listener was removed, false if the listener was not found
   */
  boolean removeListener(AuthListener listener);
}
