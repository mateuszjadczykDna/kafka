package org.apache.kafka.common.raft;

public interface ElectionStore {

    Election read();

    void write(Election latest);
}
