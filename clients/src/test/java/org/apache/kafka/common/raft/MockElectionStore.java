package org.apache.kafka.common.raft;

public class MockElectionStore implements ElectionStore {
    private Election current = Election.withUnknownLeader(0);

    @Override
    public Election read() {
        return current;
    }

    @Override
    public void write(Election update) {
        this.current = update;
    }
}
