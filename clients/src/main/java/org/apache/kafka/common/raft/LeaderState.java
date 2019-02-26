package org.apache.kafka.common.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderState extends EpochState {
    private OptionalLong highWatermark = OptionalLong.empty();
    private Map<Integer, FollowerState> followers = new HashMap<>();

    protected LeaderState(int localId, int epoch, Set<Integer> voters) {
        super(localId, epoch);

        for (int voterId : voters) {
            boolean hasEndorsedLeader = voterId == localId;
            followers.put(voterId, new FollowerState(voterId, hasEndorsedLeader, OptionalLong.empty()));
        }
    }

    @Override
    public OptionalLong highWatermark() {
        return highWatermark;
    }

    @Override
    public Election election() {
        return Election.withElectedLeader(epoch, localId);
    }

    public Set<Integer> followers() {
        return followers.keySet().stream().filter(id -> id != localId).collect(Collectors.toSet());
    }

    public Set<Integer> nonEndorsingFollowers() {
        Set<Integer> nonEndorsing = new HashSet<>();
        for (FollowerState state : followers.values()) {
            if (!state.hasEndorsedLeader)
                nonEndorsing.add(state.nodeId);
        }
        return nonEndorsing;
    }

    private void updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        ArrayList<FollowerState> followersByFetchOffset = new ArrayList<>(this.followers.values());
        Collections.sort(followersByFetchOffset);
        int indexOfHw = followers.size() / 2 - (followers.size() % 2 == 0 ? 1 : 0);
        highWatermark = followersByFetchOffset.get(indexOfHw).endOffset;
    }

    public void updateEndOffset(int remoteNodeId, long endOffset) {
        FollowerState state = ensureValidFollower(remoteNodeId);
        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset > endOffset)
                throw new IllegalArgumentException("Non-monotonic update to end offset for nodeId " + remoteNodeId);
        });
        state.hasEndorsedLeader = true;
        state.endOffset = OptionalLong.of(endOffset);
        updateHighWatermark();
    }

    public void addEndorsementFrom(int remoteNodeId) {
        FollowerState followerState = ensureValidFollower(remoteNodeId);
        followerState.hasEndorsedLeader = true;
    }

    private FollowerState ensureValidFollower(int remoteNodeId) {
        FollowerState state = followers.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected endorsement from non-voter " + remoteNodeId);
        return state;
    }

    public void updateLocalEndOffset(long endOffset) {
        updateEndOffset(localId, endOffset);
    }

    private static class FollowerState implements Comparable<FollowerState> {
        final int nodeId;
        boolean hasEndorsedLeader;
        OptionalLong endOffset;

        public FollowerState(int nodeId,
                             boolean hasEndorsedLeader,
                             OptionalLong endOffset) {
            this.nodeId = nodeId;
            this.hasEndorsedLeader = hasEndorsedLeader;
            this.endOffset = endOffset;
        }

        @Override
        public int compareTo(FollowerState that) {
            if (this.endOffset == that.endOffset)
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return -1;
            else if (!that.endOffset.isPresent())
                return 1;
            else
                return Long.compare(this.endOffset.getAsLong(), that.endOffset.getAsLong());
        }
    }

}
