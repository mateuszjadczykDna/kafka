package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

public class NoOpStateMachine implements DistributedStateMachine {
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);

    @Override
    public void becomeLeader(int epoch) {

    }

    @Override
    public void becomeFollower(int epoch) {

    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    @Override
    public synchronized boolean accept(Records records) {
        return true;
    }
}
