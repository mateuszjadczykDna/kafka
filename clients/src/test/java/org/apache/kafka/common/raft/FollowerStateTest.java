package org.apache.kafka.common.raft;

import org.junit.Test;

import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FollowerStateTest {
    private final int localId = 0;
    private final int epoch = 5;

    @Test
    public void testVoteForCandidate() {
        FollowerState state = new FollowerState(localId, epoch);
        assertTrue(state.assertNotAttached());
        assertFalse(state.hasVoted());

        int votedId = 1;
        assertTrue(state.grantVoteTo(votedId));
        assertTrue(state.hasVoted());
        assertTrue(state.isVotedCandidate(votedId));
    }

    @Test
    public void testCannotChangeVote() {
        FollowerState state = new FollowerState(localId, epoch);
        int votedId = 1;
        int otherCandidateId = 2;
        assertTrue(state.grantVoteTo(votedId));
        assertThrows(IllegalArgumentException.class, () -> state.grantVoteTo(otherCandidateId));
    }

    @Test
    public void testIdempotentVote() {
        FollowerState state = new FollowerState(localId, epoch);
        int votedId = 1;
        assertTrue(state.grantVoteTo(votedId));
        assertFalse(state.grantVoteTo(votedId));
        assertTrue(state.hasVoted());
        assertTrue(state.isVotedCandidate(votedId));
    }

    @Test
    public void testCannotVoteIfLeaderIsKnown() {
        FollowerState state = new FollowerState(localId, epoch);
        int leaderId = 1;
        int candidateId = 2;
        state.acknowledgeLeader(leaderId);
        assertFalse(state.hasVoted());
        assertThrows(IllegalArgumentException.class, () -> state.grantVoteTo(candidateId));
        assertFalse(state.hasVoted());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testAckLeaderWithoutVoting() {
        FollowerState state = new FollowerState(localId, epoch);
        int leaderId = 1;
        state.acknowledgeLeader(leaderId);
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testAckLeaderAfterVoting() {
        FollowerState state = new FollowerState(localId, epoch);
        int candidateId = 1;
        int leaderId = 2;
        assertTrue(state.grantVoteTo(candidateId));
        assertTrue(state.acknowledgeLeader(leaderId));
        assertFalse(state.isVotedCandidate(candidateId));
        assertFalse(state.hasVoted());
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testCannotChangeLeader() {
        FollowerState state = new FollowerState(localId, epoch);
        int leaderId = 1;
        int otherLeaderId = 2;
        assertTrue(state.acknowledgeLeader(leaderId));
        assertThrows(IllegalArgumentException.class, () -> state.acknowledgeLeader(otherLeaderId));
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testIdempotentLeaderAcknowledgement() {
        FollowerState state = new FollowerState(localId, epoch);
        int leaderId = 1;
        assertTrue(state.acknowledgeLeader(leaderId));
        assertFalse(state.acknowledgeLeader(leaderId));
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testUpdateHighWatermarkOnlyPermittedWithLeader() {
        OptionalLong highWatermark = OptionalLong.of(15L);
        FollowerState state = new FollowerState(localId, epoch);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(highWatermark));
        int candidateId = 1;
        assertTrue(state.grantVoteTo(candidateId));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(highWatermark));
        int leaderId = 2;
        assertTrue(state.acknowledgeLeader(leaderId));
        state.updateHighWatermark(highWatermark);
        assertEquals(highWatermark, state.highWatermark());
    }

    @Test
    public void testMonotonicHighWatermark() {
        OptionalLong highWatermark = OptionalLong.of(15L);
        FollowerState state = new FollowerState(localId, epoch);
        int leaderId = 1;
        assertTrue(state.acknowledgeLeader(leaderId));
        state.updateHighWatermark(highWatermark);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.empty()));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.of(14L)));
        state.updateHighWatermark(highWatermark);
        assertEquals(highWatermark, state.highWatermark());
    }

}