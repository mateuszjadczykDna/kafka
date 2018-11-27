/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

public enum TierPartitionStatus {
    // TierPartitionStatus object has been initialized
    // but it is not yet backed by a file on disk.
    INIT,
    // Disk is currently offline.
    // Used for JBOD support.
    DISK_OFFLINE,
    // TierPartitionState has been added, but it is currently
    // being materialized by the join consumer.
    CATCHUP,
    // TierPartitionState status is being continuously materialized
    // by the primary consumer.
    ONLINE
}
