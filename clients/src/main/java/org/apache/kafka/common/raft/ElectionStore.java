package org.apache.kafka.common.raft;

import java.io.IOException;

public interface ElectionStore {

    /**
     * Read the latest election state.
     * @return The latest written election state or `ElectionState.withUnknownLeader(0)` if there is none.
     * @throws IOException For any error encountered reading from the storage
     */
    ElectionState read() throws IOException;

    /**
     * Persist the updated election state. This must be atomic, both writing the full updated state
     * and replacing the old state.
     * @param latest The latest election state
     * @throws IOException For any error encountered while writing the updated state
     */
    void write(ElectionState latest) throws IOException;
}
