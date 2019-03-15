package org.apache.kafka.common.raft;

/**
 * Encapsulate election state stored on disk after every state change.
 */
public class Election {
    public final int epoch;
    private final int leaderIdOrNil;
    private final int votedIdOrNil;

    private Election(int epoch,
                     int leaderIdOrNil,
                     int votedIdOrNil) {
        this.epoch = epoch;
        this.leaderIdOrNil = leaderIdOrNil;
        this.votedIdOrNil = votedIdOrNil;
    }

    public static Election withVotedCandidate(int epoch, int votedId) {
        if (votedId < 0)
            throw new IllegalArgumentException("Illegal voted Id " + votedId + ": must be non-negative");
        return new Election(epoch, -1, votedId);
    }

    public static Election withElectedLeader(int epoch, int leaderId) {
        if (leaderId < 0)
            throw new IllegalArgumentException("Illegal leader Id " + leaderId + ": must be non-negative");
        return new Election(epoch, leaderId, -1);
    }

    public static Election withUnknownLeader(int epoch) {
        return new Election(epoch, -1, -1);
    }

    public boolean isLeader(int nodeId) {
        if (nodeId < 0)
            throw new IllegalArgumentException();
        return leaderIdOrNil == nodeId;
    }

    public boolean isCandidate(int nodeId) {
        return votedIdOrNil == nodeId;
    }

    public boolean isFollower(int nodeId) {
        return !isLeader(nodeId) && !isCandidate(nodeId);
    }

    public int leaderId() {
        if (leaderIdOrNil < 0)
            throw new IllegalStateException("Attempt to access nil leaderId");
        return leaderIdOrNil;
    }

    public int votedId() {
        if (votedIdOrNil < 0)
            throw new IllegalStateException("Attempt to access nil votedId");
        return votedIdOrNil;
    }

    public boolean hasLeader() {
        return leaderIdOrNil >= 0;
    }

    public boolean hasVoted() {
        return votedIdOrNil >= 0;
    }


    @Override
    public String toString() {
        return "Election(epoch=" + epoch +
                ", leaderIdOrNil=" + leaderIdOrNil +
                ", votedIdOrNil=" + votedIdOrNil +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Election election = (Election) o;

        if (epoch != election.epoch) return false;
        if (leaderIdOrNil != election.leaderIdOrNil) return false;
        return votedIdOrNil == election.votedIdOrNil;
    }

    @Override
    public int hashCode() {
        int result = epoch;
        result = 31 * result + leaderIdOrNil;
        result = 31 * result + votedIdOrNil;
        return result;
    }
}
