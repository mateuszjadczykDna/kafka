package org.apache.kafka.common.raft;

public class EndOffset implements Comparable<EndOffset> {
    public final long offset;
    public final int epoch;

    public EndOffset(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    @Override
    public int compareTo(EndOffset that) {
        if (this.epoch == that.epoch)
            return Long.compare(this.offset, that.offset);
        return Integer.compare(this.epoch, that.epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndOffset endOffset = (EndOffset) o;

        if (offset != endOffset.offset) return false;
        return epoch == endOffset.epoch;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + epoch;
        return result;
    }

    @Override
    public String toString() {
        return "EndOffset(" +
                "offset=" + offset +
                ", epoch=" + epoch +
                ')';
    }
}
