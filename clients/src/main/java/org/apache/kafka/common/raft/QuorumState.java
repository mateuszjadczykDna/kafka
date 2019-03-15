package org.apache.kafka.common.raft;

import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is responsible for managing the current state of this node and ensuring only
 * valid state transitions.
 */
public class QuorumState {
    public final int localId;
    private final Logger log;
    private final ElectionStore store;
    private final Set<Integer> voters;
    private EpochState state;

    public QuorumState(int localId,
                       Set<Integer> voters,
                       ElectionStore store,
                       LogContext logContext) {
        this.localId = localId;
        this.voters = new HashSet<>(voters);
        this.store = store;
        this.log = logContext.logger(QuorumState.class);
    }

    public void initialize() {
        Election election = store.read();
        state = new FollowerState(localId, election.epoch);
        if (election.hasLeader() && election.leaderId() != localId) {
            becomeFollower(election.epoch, election.leaderId());
        } else if (election.hasVoted() && election.votedId() != localId) {
            becomeVotedFollower(election.epoch, election.votedId());
        } else if (isVoter()) {
            becomeCandidate();
        } else {
            becomeUnattachedFollower(election.epoch);
        }
    }

    public Set<Integer> remoteVoters() {
        return voters.stream().filter(voterId -> voterId != localId).collect(Collectors.toSet());
    }

    public int epoch() {
        return state.epoch;
    }

    public int leaderIdOrNil() {
        return leaderId().orElse(-1);
    }

    public OptionalLong highWatermark() {
        return state.highWatermark();
    }

    public OptionalInt leaderId() {
        Election election = state.election();
        if (election.hasLeader())
            return OptionalInt.of(state.election().leaderId());
        else
            return OptionalInt.empty();
    }

    public boolean isLeader() {
        return state instanceof LeaderState;
    }

    public boolean isCandidate() {
        return state instanceof CandidateState;
    }

    public boolean isFollower() {
        return state instanceof FollowerState;
    }

    public boolean isVoter() {
        return voters.contains(localId);
    }

    public boolean isVoter(int nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isObserver() {
        return !isVoter();
    }

    public boolean becomeUnattachedFollower(int epoch) {
        return becomeFollower(epoch, FollowerState::assertNotAttached);
    }

    public boolean becomeVotedFollower(int epoch, int candidateId) {
        if (!isVoter(candidateId))
            throw new IllegalArgumentException("Cannot become follower of non-voter " + candidateId);
        return becomeFollower(epoch, state -> state.grantVoteTo(candidateId));
    }

    public boolean becomeFollower(int epoch, int leaderId) {
        if (!isVoter(leaderId))
            throw new IllegalArgumentException("Cannot become follower of non-voter " + leaderId);
        return becomeFollower(epoch, state -> state.acknowledgeLeader(leaderId));
    }

    private boolean becomeFollower(int newEpoch, Function<FollowerState, Boolean> func) {
        int currentEpoch = epoch();
        boolean stateChanged = false;

        if (newEpoch < currentEpoch) {
            throw new IllegalArgumentException("Cannot become follower in epoch " + newEpoch +
                    " since it is smaller epoch than our current epoch " + currentEpoch);
        } else if (newEpoch > currentEpoch || isCandidate()) {
            state = new FollowerState(localId, newEpoch);
            stateChanged = true;
        } else if (isLeader()) {
            throw new IllegalArgumentException("Cannot become leader of epoch " + newEpoch +
                    " since we are already the leader of this epoch");
        }

        FollowerState followerState = followerStateOrThrow();
        if (func.apply(followerState) || stateChanged) {
            log.info("Becoming follower in epoch {}", newEpoch);
            store.write(followerState.election());
            return true;
        }
        return false;
    }

    public CandidateState becomeCandidate() {
        if (isObserver())
            throw new IllegalStateException("Cannot become candidate since we are not a voter");
        if (isLeader())
            throw new IllegalStateException("Cannot become candidate after being leader");

        int newEpoch = epoch() + 1;
        log.info("Becoming candidate in epoch {}", newEpoch);
        CandidateState state = new CandidateState(localId, newEpoch, voters);
        store.write(state.election());
        this.state = state;
        return state;
    }

    public LeaderState becomeLeader() {
        if (isObserver())
            throw new IllegalStateException("Cannot become candidate since we are not a voter");

        CandidateState candidateState = candidateStateOrThrow();
        if (!candidateState.isVoteGranted())
            throw new IllegalStateException("Cannot become leader without majority votes granted");

        log.info("Becoming leader in epoch {}", epoch());
        LeaderState state = new LeaderState(localId, epoch(), voters);
        store.write(state.election());
        this.state = state;
        return state;
    }

    public FollowerState followerStateOrThrow() {
        if (isFollower())
            return (FollowerState) state;
        throw new IllegalStateException("Expected to be a follower, but current state is " + state);
    }

    public LeaderState leaderStateOrThrow() {
        if (isLeader())
            return (LeaderState) state;
        throw new IllegalStateException("Expected to be a leader, but current state is " + state);
    }

    public CandidateState candidateStateOrThrow() {
        if (isCandidate())
            return (CandidateState) state;
        throw new IllegalStateException("Expected to be a candidate, but current state is " + state);
    }

    public interface Visitor<T> {
        T ifFollower(FollowerState state);
        T ifLeader(LeaderState state);
        T ifCandidate(CandidateState state);
    }

    public interface VoidVisitor {
        void ifFollower(FollowerState state);
        void ifLeader(LeaderState state);
        void ifCandidate(CandidateState state);
    }

    public <T> T visit(Visitor<T> visitor) {
        if (isCandidate())
            return visitor.ifCandidate((CandidateState) state);
        else if (isLeader())
            return visitor.ifLeader((LeaderState) state);
        else
            return visitor.ifFollower((FollowerState) state);
    }

    public void visit(VoidVisitor visitor) {
        if (isCandidate())
            visitor.ifCandidate((CandidateState) state);
        else if (isLeader())
            visitor.ifLeader((LeaderState) state);
        else
            visitor.ifFollower((FollowerState) state);
    }

}
