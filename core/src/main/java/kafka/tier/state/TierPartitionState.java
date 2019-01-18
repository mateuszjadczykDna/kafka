/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.NavigableSet;
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
     * The topic-partition corresponding to this TierPartition.
     * @return topic-partition
     */
    TopicPartition topicPartition();

    /**
     * The directory where the TierPartition is stored on disk.
     * @return file handle for the directory
     */
    File dir();

    /**
     * Path to where the TierPartition is stored on disk.
     * @return path
     */
    String path();

    /**
     * Determine start offset spanned by the TierPartitionState.
     * @return start offset
     * @throws IOException
     */
    OptionalLong startOffset() throws IOException;

    /**
     * Determine end offset spanned by the TierPartitionState.
     * @return end offset
     * @throws IOException
     */
    OptionalLong endOffset() throws IOException;

    /**
     * Scan the ObjectMetadata (segment) entries in this tier partition, and return the count.
     * @return number of tiered segments
     */
    int numSegments();

    /**
     * Get the set of base offsets for all tiered segments. The returned set is sorted by base offset.
     * @return Set of base offset for tiered segments
     */
    NavigableSet<Long> segmentOffsets();

    /**
     * Get the set of base offsets for all tiered segments in a given range. The returned set is sorted by base offset.
     * @param from Start of the range, include segment which contains "from" (inclusive)
     * @param to End of the range, upper bound exclusive offset to include or the end of the log if "to" is past the end
     * @return Set of base offset for tiered segments
     */
    NavigableSet<Long> segmentOffsets(long from, long to) throws IOException;

    /**
     * Lookup the TierObjectMetadata which will contain data for a target offset.
     * @param targetOffset the target offset to lookup the overlapping or next metadata for.
     * @return The TierObjectMetadata, if any.
     * @throws IOException if disk error encountered
     */
    Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException;

    /**
     * Appends abstract metadata to the tier partition.
     * Dispatches to more specific append method.
     * When appending a TierTopicInitLeader entry, it may advance the tierEpoch.
     * When appending a TierObjectMetadata entry, it may append the tier metadata to the tier
     * partition log file.
     * @param tierMetadata AbstractTierMetadata entry to be appended to the tier partition log.
     * @return Returns an AppendResult denoting the result of the append action.
     * @throws IOException
     */
    AppendResult append(AbstractTierMetadata tierMetadata) throws IOException;

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
    int tierEpoch() throws IOException;

    boolean tieringEnabled();

    /**
     * Called when tiering is enabled for this tier topic partition.
     * @throws IOException
     */
    void onTieringEnable() throws IOException;

    /**
     * flush data contained in this TierPartitionState to disk.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Begin catchup phase for tier partition state.
     */
    void beginCatchup();

    /**
     * Mark catchup completed for tier partition state.
     */
    void onCatchUpComplete();

    /**
     * Return the current status of the TierPartitionState.
     * @return TierPartitionStatus
     */
    TierPartitionStatus status();

    /**
     * Update the directory reference for the log and indices in this segment. This would typically be called after a
     * directory is renamed.
     * @param dir The new directory
     */
    void updateDir(File dir);

    /**
     * Delete this TierPartitionState from local storage.
     * @throws IOException
     */
    void delete() throws IOException;

    /**
     * Close TierPartition, flushing to disk.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     *
     * @throws IOException
     */
    void closeHandlers() throws IOException;
}
