package org.apache.kafka.common.raft;

import java.util.OptionalLong;

public class FollowerState extends EpochState {
    private int leaderIdOrNil;
    private int votedIdOrNil;
    private OptionalLong highWatermark;

    public FollowerState(int localId, int epoch) {
        super(localId, epoch);
        leaderIdOrNil = -1;
        votedIdOrNil = -1;
        highWatermark = OptionalLong.empty();
    }

    @Override
    public OptionalLong highWatermark() {
        return highWatermark;
    }

    @Override
    public Election election() {
        if (hasVoted())
            return Election.withVotedCandidate(epoch, votedIdOrNil);
        if (hasLeader())
            return Election.withElectedLeader(epoch, leaderIdOrNil);
        return Election.withUnknownLeader(epoch);
    }


    /**
     * Grant a vote to the candidate. The vote is permitted only if we had already voted for
     * the candidate or if we have no current leader and have not voted in this epoch.
     *
     * @param candidateId The candidate we are voting for
     * @return true if we had not already cast our vote
     */
    public boolean grantVoteTo(int candidateId) {
        if (candidateId < 0) {
            throw new IllegalArgumentException("Illegal negative candidateId: " + candidateId);
        } else if (hasLeader()) {
            throw new IllegalArgumentException("Cannot vote in epoch " + epoch +
                    " since we already have a known leader for epoch");
        } else if (hasVoted()) {
            if (votedIdOrNil != candidateId) {
                throw new IllegalArgumentException("Cannot change vote in epoch " + epoch +
                        " from " + votedIdOrNil + " to " + candidateId);
            }
            return false;
        }

        this.votedIdOrNil = candidateId;
        return true;
    }

    public boolean hasLeader() {
        return leaderIdOrNil >= 0;
    }

    public boolean acknowledgeLeader(int leaderId) {
        if (leaderId < 0) {
            throw new IllegalArgumentException("Invalid negative leaderId: " + leaderId);
        } if (hasLeader()) {
            if (leaderIdOrNil != leaderId) {
                throw new IllegalArgumentException("Cannot acknowledge leader " + leaderId +
                        " in epoch " + epoch + " since we have already acknowledged " + leaderIdOrNil);
            }
            return false;
        }

        votedIdOrNil = -1;
        leaderIdOrNil = leaderId;
        return true;
    }

    public int leaderId() {
        if (!hasLeader()) {
            throw new IllegalArgumentException("Cannot access leaderId of epoch " + epoch +
                    " since we do not know it");
        }
        return leaderIdOrNil;
    }

    public int votedId() {
        if (hasLeader()) {
            throw new IllegalArgumentException("Cannot access votedId of epoch " + epoch +
                    " since we already have a leader");
        }
        if (!hasVoted()) {
            throw new IllegalArgumentException("Cannot access votedId of epoch " + epoch +
                    " because we have not voted");

        }
        return votedIdOrNil;
    }

    public boolean hasVoted() {
        return votedIdOrNil >= 0;
    }

    public boolean isVotedCandidate(int candidateId) {
        return hasVoted() && votedIdOrNil == candidateId;
    }

    public void updateHighWatermark(OptionalLong highWatermark) {
        if (!hasLeader())
            throw new IllegalArgumentException("Cannot update high watermark without an acknowledged leader");
        if (!highWatermark.isPresent() && this.highWatermark.isPresent())
            throw new IllegalArgumentException("Attempt to overwrite current high watermark " + highWatermark +
                    " with unknown value");
        this.highWatermark.ifPresent(hw -> {
            if (hw > highWatermark.getAsLong()) {
                throw new IllegalArgumentException("Non-monotonic update of high watermark attempted");
            }
        });

        this.highWatermark = highWatermark;
    }

    public boolean assertNotAttached() {
        if (hasLeader())
            throw new IllegalArgumentException("Unattached assertion failed since we have a current leader");
        if (hasVoted())
            throw new IllegalArgumentException("Unattached assertion failed since we have a voted candidate");
        return true;
    }

}
