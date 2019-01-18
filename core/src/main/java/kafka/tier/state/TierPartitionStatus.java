/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

public enum TierPartitionStatus {
    // TierPartitionState has been initialized but it is not yet backed by a file on disk
    CLOSED,
    // TierPartitionState has been initialized, the file is open but is in read-only mode
    READ_ONLY,
    // TierPartitionState has been initialized and is open for read/write, but is currently being materialized by the catchup consumer.
    CATCHUP,
    // TierPartitionState has been initialized and is open for read/write. It is being continuously materialized by the primary consumer.
    ONLINE,
    // Disk is offline. Used for JBOD support.
    DISK_OFFLINE;

    public boolean isOpen() {
        return this == READ_ONLY || this == CATCHUP || this == ONLINE;
    }

    public boolean isOpenForWrite() {
        return this == CATCHUP || this == ONLINE;
    }
}
