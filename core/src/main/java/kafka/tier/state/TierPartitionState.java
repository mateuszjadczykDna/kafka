/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.serdes.ObjectMetadata;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;


public interface TierPartitionState {
    /**
     * The result of an attempt to append a tier metadata entry.
     */
    enum AppendResult {
        // the tier partition status has not been initialized
        ILLEGAL,
        // the entry was materialized but was fenced
        FENCED,
        // the entry was materialized
        ACCEPTED
    }

    /**
     * Determine end offset spanned by the TierPartitionState.
     * @return end offset
     * @throws IOException
     */
    OptionalLong endOffset() throws IOException;

    /**
     * Determine beginning offset spanned by the TierPartitionState.
     * @return beginning offset
     * @throws IOException
     */
    OptionalLong beginningOffset() throws IOException;

    /**
     * Sum the size of all segment spanned by this TierPartitionState.
     * @return total size
     * @throws IOException
     */
    long totalSize() throws IOException;

    /**
     * Return the current tierEpoch.
     * Metadata will only be added to TierPartitionState if the metadata's
     * tierEpoch is equal to the TierPartitionState's tierEpoch.
     * @return tierEpoch
     */
    int tierEpoch();

    /**
     * Appends abstract metadata to the tier partition.
     * Dispatches to more specific append method.
     * When appending a TierTopicInitLeader entry, it may advance the tierEpoch.
     * When appending a TierObjectMetadata entry, it may append the tier metadata to the tier
     * partition log file.
     * @param entry AbstractTierMetadata entry to be appended to the tier partition log.
     * @return Returns an AppendResult denoting the result of the append action.
     * @throws IOException
     */
    AppendResult append(AbstractTierMetadata entry) throws IOException;

    /**
     * Lookup the TierObjectMetadata which will contain data for a target offset.
     * @param targetOffset the target offset to lookup the overlapping or next metadata for.
     * @return The TierObjectMetadata, if any.
     * @throws IOException if disk error encountered
     */
    Optional<TierObjectMetadata> getObjectMetadataForOffset(long targetOffset)
            throws IOException;

    /**
     * Path to where the TierPartition is stored on disk.
     * @return path
     */
    String path();

    /**
     * The topic corresponding to this TierPartition.
     * @return topic
     */
    String topic();

    /**
     * The partition corresponding to this TierPartition.
     * @return partition
     */
    int partition();

    /**
     * @return ObjectMetadata iterator corresponding to entries contained in this TierPartition
     * @throws java.io.IOException
     */
    Iterator<ObjectMetadata> iterator() throws java.io.IOException;

    /**
     * Transition TierPartitionState to a new TierPartitionStatus
     * @param status The new status
     * @throws IOException
     */
    void targetStatus(TierPartitionStatus status) throws IOException;

    /**
     * Return the current status of the TierPartitionState.
     * @return TierPartitionStatus
     */
    TierPartitionStatus status();

    /**
     * Scan the ObjectMetadata (segment) entries in this tier partition, and return the count.
     * @return number of tiered segments
     * @throws IOException
     */
    long numSegments() throws IOException;

    /**
     * flush data contained in this TierPartitionState to disk.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Close TierPartition, flushing to disk.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Delete this TierPartitionState from storage.
     * @throws IOException
     */
    void delete() throws IOException;
}
