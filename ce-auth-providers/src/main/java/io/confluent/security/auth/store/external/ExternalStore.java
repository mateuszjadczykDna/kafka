// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.external;

import org.apache.kafka.common.Configurable;

/**
 * Interface for managing metadata from  an external store, for example an LDAP server.
 */
public interface ExternalStore extends Configurable {

  /**
   * Starts consuming metadata from the external store.
   *
   * @param generationId The generation id of the writer
   */
  void start(int generationId);

  /**
   * Stops consuming metadata from the external store if the provided generation
   * id is null or equal to the current generation id of this store.
   * @param generationId The generation id to match or null to match any generation
   */
  void stop(Integer generationId);

  /**
   * Returns true if the external store has failed and retries have timed out.
   */
  boolean failed();
}
