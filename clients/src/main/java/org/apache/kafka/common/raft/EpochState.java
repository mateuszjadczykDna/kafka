package org.apache.kafka.common.raft;

import java.util.OptionalLong;

public interface EpochState {

    default OptionalLong highWatermark() {
        return OptionalLong.empty();
    }

    ElectionState election();

    int epoch();

}
