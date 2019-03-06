// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import io.confluent.security.store.kafka.coordinator.MetadataServiceCoordinator;

public interface Writer {

  /**
   * Starts master writer with the specified generation id. Writer generation is determined
   * by the {@link MetadataServiceCoordinator} during writer election.
   *
   * @param generationId Generation id of writer
   */
  void startWriter(int generationId);

  /**
   * Stops this writer because a new master writer was elected. If `generationId` is null,
   * the writer is stopped regardless of the current generation of the writer. If not, the
   * writer is stopped only if its current generation matches the provided value.
   *
   * @param generationId Generation id of writer being stopped or null to stop regardless of
   *                     current writer generation
   */
  void stopWriter(Integer generationId);

  /**
   * Returns true if this is the master writer and is ready to process requests
   */
  boolean ready();
}
