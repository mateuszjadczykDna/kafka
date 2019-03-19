package org.apache.kafka.common.raft;

public class OffsetAndEpoch implements Comparable<OffsetAndEpoch> {
    public final long offset;
    public final int epoch;

    public OffsetAndEpoch(long offset, int epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetAndEpoch that = (OffsetAndEpoch) o;

        if (offset != that.offset) return false;
        return epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + epoch;
        return result;
    }

    @Override
    public String toString() {
        return "OffsetAndEpoch(" +
                "offset=" + offset +
                ", epoch=" + epoch +
                ')';
    }

    @Override
    public int compareTo(OffsetAndEpoch o) {
        if (epoch == o.epoch)
            return Long.compare(offset, o.offset);
        return Integer.compare(epoch, o.epoch);
    }
}
