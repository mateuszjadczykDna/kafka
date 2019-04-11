package org.apache.kafka.common.raft;

public class MockElectionStore implements ElectionStore {
    private ElectionState current = ElectionState.withUnknownLeader(0);

    @Override
    public ElectionState read() {
        return current;
    }

    @Override
    public void write(ElectionState update) {
        this.current = update;
    }
}
