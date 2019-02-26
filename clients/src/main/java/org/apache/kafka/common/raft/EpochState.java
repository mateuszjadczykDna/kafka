package org.apache.kafka.common.raft;

import java.util.OptionalLong;

public abstract class EpochState {
    public final int localId;
    public final int epoch;

    protected EpochState(int localId, int epoch) {
        this.localId = localId;
        this.epoch = epoch;
    }

    public OptionalLong highWatermark() {
        return OptionalLong.empty();
    }

    public abstract Election election();

}
