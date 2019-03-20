package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.Records;

/**
 * First try at an interface for state machines on top of Raft. At any time, the state
 * machine is either following or leading the Raft quorum. If leading, then all new
 * record appends will be routed to the leader, which will have an opportunity to either
 * accept or reject the append attempt.
 */
public interface DistributedStateMachine {

    /**
     * Become a leader. This is invoked after a new election in the quorum if this
     * node was elected as the leader.
     *
     * @param epoch The latest quorum epoch
     */
    void becomeLeader(int epoch);

    /**
     * Become a follower. This is invoked after a new election finishes if this
     * node was not elected as the leader.
     *
     * @param epoch The latest quorum epoch
     */
    void becomeFollower(int epoch);

    /**
     * The next expected offset that will be appended to the log. This should be
     * updated after every call to {@link #apply(Records)}.
     *
     * @return The offset and epoch that was last consumed
     */
    OffsetAndEpoch position();

    /**
     * Apply committed records to the state machine.
     *
     * @param records The records to apply
     */
    void apply(Records records);

    /**
     * This is only invoked by leaders. The leader is guaranteed to have the full committed
     * state before this method is invoked in a new leader epoch.
     *
     * Note that acceptance does not guarantee that the records will become committed
     * since that depends on replication to the quorum. If there is a leader change,
     * accepted records may be lost.
     *
     * @param records The recor
     * @return true if the records should be appended to the log
     */
    boolean accept(Records records);
}
