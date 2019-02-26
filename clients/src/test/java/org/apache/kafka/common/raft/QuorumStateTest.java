package org.apache.kafka.common.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final MockElectionStore store = new MockElectionStore();

    @Test
    public void testInitializePrimordialEpoch() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(0, store.read().epoch);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch);
    }

    @Test
    public void testInitializeAsFollowerWithElectedLeader() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.write(Election.withElectedLeader(epoch, node1));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasLeader());
        assertEquals(epoch, followerState.epoch);
        assertEquals(node1, followerState.leaderId());
    }

    @Test
    public void testInitializeAsFollowerWithVotedCandidate() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.write(Election.withVotedCandidate(epoch, node1));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertEquals(epoch, followerState.epoch);
        assertEquals(node1, followerState.votedId());
    }

    @Test
    public void testInitializeAsFormerCandidate() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.write(Election.withVotedCandidate(epoch, localId));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isCandidate());
        assertEquals(epoch + 1, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch + 1, candidateState.epoch);
        assertEquals(Utils.mkSet(node1, node2), candidateState.remainingVoters());
    }

    @Test
    public void testInitializeAsFormerLeader() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.write(Election.withElectedLeader(epoch, localId));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isCandidate());
        assertEquals(epoch + 1, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch + 1, candidateState.epoch);
        assertEquals(Utils.mkSet(node1, node2), candidateState.remainingVoters());
    }

    @Test
    public void testBecomeLeader() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(0, store.read().epoch);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isCandidate());

        LeaderState leaderState = state.becomeLeader();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch);
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @Test
    public void testCannotBecomeLeaderIfAlreadyLeader() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(0, store.read().epoch);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeLeader();
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, state::becomeLeader);
        assertTrue(state.isLeader());
    }

    @Test
    public void testCannotBecomeLeaderIfCurrentlyFollowing() {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.write(Election.withVotedCandidate(epoch, leaderId));
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, state::becomeLeader);
    }

    @Test
    public void testCannotBecomeCandidateIfCurrentlyLeading() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(0, store.read().epoch);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeLeader();
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
    }

    @Test
    public void testCannotBecomeLeaderWithoutGrantedVote() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, state::becomeLeader);
        state.candidateStateOrThrow().voteGrantedBy(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.becomeLeader();
        assertTrue(state.isLeader());
    }

    @Test
    public void testLeaderToFollowerOfElectedLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.candidateStateOrThrow().voteGrantedBy(otherNodeId);
        state.becomeLeader();
        assertTrue(state.becomeFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(Optional.of(otherNodeId), state.leaderId());
        assertEquals(Election.withElectedLeader(5, otherNodeId), store.read());
    }

    @Test
    public void testLeaderToUnattachedFollower() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.candidateStateOrThrow().voteGrantedBy(otherNodeId);
        state.becomeLeader();
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(Optional.empty(), state.leaderId());
        assertEquals(Election.withUnknownLeader(5), store.read());
    }

    @Test
    public void testLeaderToFollowerOfVotedCandidate() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.candidateStateOrThrow().voteGrantedBy(otherNodeId);
        state.becomeLeader();
        assertTrue(state.becomeVotedFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(Optional.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.isVotedCandidate(otherNodeId));
        assertEquals(Election.withVotedCandidate(5, otherNodeId), store.read());
    }

    @Test
    public void testCandidateToFollowerOfElectedLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.becomeFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(Optional.of(otherNodeId), state.leaderId());
        assertEquals(Election.withElectedLeader(5, otherNodeId), store.read());
    }

    @Test
    public void testCandidateToUnattachedFollower() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(Optional.empty(), state.leaderId());
        assertEquals(Election.withUnknownLeader(5), store.read());
    }

    @Test
    public void testCandidateToFollowerOfVotedCandidate() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.becomeVotedFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(Optional.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.isVotedCandidate(otherNodeId));
        assertEquals(Election.withVotedCandidate(5, otherNodeId), store.read());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateSameEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(5, otherNodeId);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch);
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.isVotedCandidate(otherNodeId));
        assertEquals(Election.withVotedCandidate(5, otherNodeId), store.read());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateHigherEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(8, otherNodeId);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch);
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.isVotedCandidate(otherNodeId));
        assertEquals(Election.withVotedCandidate(8, otherNodeId), store.read());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeVotedFollower(5, node1);
        state.becomeFollower(5, node2);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch);
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(Election.withElectedLeader(5, node2), store.read());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeVotedFollower(5, node1);
        state.becomeFollower(8, node2);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch);
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(Election.withElectedLeader(8, node2), store.read());
    }

    @Test
    public void testFollowerCannotChangeVotesInSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeVotedFollower(5, node1);
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(5, node2));
        FollowerState followerState = state.followerStateOrThrow();
        assertFalse(followerState.hasLeader());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.isVotedCandidate(node1));
        assertEquals(Election.withVotedCandidate(5, node1), store.read());
    }

    @Test
    public void testFollowerCannotChangeLeadersInSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeFollower(8, node2);
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(8, node1));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch);
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(Election.withElectedLeader(8, node2), store.read());
    }

    @Test
    public void testFollowerOfElectedLeaderHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeFollower(8, node2);
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(8, node1));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch);
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(Election.withElectedLeader(8, node2), store.read());
    }

    @Test
    public void testCannotTransitionFromFollowerToLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeUnattachedFollower(5);
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(Election.withUnknownLeader(5), store.read());
    }

    @Test
    public void testCannotTransitionFromCandidateToLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(Election.withVotedCandidate(6, localId), store.read());
    }

    @Test
    public void testCannotTransitionFromLeaderToLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        state.candidateStateOrThrow().voteGrantedBy(otherNodeId);
        state.becomeLeader();
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(Election.withElectedLeader(6, localId), store.read());
    }

    @Test
    public void testCannotBecomeFollowerOfNonVoter() {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFollower(4, nonVoterId));
    }

    @Test
    public void testObserverCannotBecomeCandidateCandidateOrLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        assertTrue(state.isObserver());
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
        assertThrows(IllegalStateException.class, state::becomeLeader);
    }

}