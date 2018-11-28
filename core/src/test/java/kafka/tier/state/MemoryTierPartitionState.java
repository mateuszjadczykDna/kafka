/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MemoryTierPartitionState implements TierPartitionState {
    private final CopyOnWriteArrayList<TierObjectMetadata> objectMetadataEntries
            = new CopyOnWriteArrayList<>();
    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final String topic;
    private final int partition;
    private volatile TierPartitionStatus status;

    public MemoryTierPartitionState(TopicPartition topicPartition) {
        this.status = TierPartitionStatus.INIT;
        this.topic = topicPartition.topic();
        this.partition = topicPartition.partition();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public OptionalLong endOffset() {
        if (objectMetadataEntries.size() > 0) {
            TierObjectMetadata finalEntry =
                    objectMetadataEntries.get(objectMetadataEntries.size() - 1);
            return OptionalLong.of(finalEntry.endOffset());
        } else {
            return OptionalLong.empty();
        }
    }

    public OptionalLong beginningOffset() {
        if (objectMetadataEntries.size() > 0) {
            return OptionalLong.of(objectMetadataEntries.get(0).startOffset());
        } else {
            return OptionalLong.empty();
        }
    }

    public Iterator<ObjectMetadata> iterator() {
        return objectMetadataEntries
                .stream()
                .map(TierObjectMetadata::objectMetadata)
                .collect(Collectors.toList())
                .iterator();
    }

    public TierPartitionStatus status() {
        return status;
    }

    public void targetStatus(TierPartitionStatus status) {
        this.status = status;
    }

    public long totalSize() {
        return objectMetadataEntries
                .stream()
                .map(TierObjectMetadata::size)
                .mapToInt(Integer::intValue).sum();
    }

    public List<TierObjectMetadata> getEntries() {
        return objectMetadataEntries;
    }

    public int tierEpoch() {
        return currentEpoch.get();
    }

    public TierPartitionState.AppendResult append(AbstractTierMetadata entry) {
        if (status == TierPartitionStatus.INIT) {
            return AppendResult.ILLEGAL;
        } else if (entry instanceof TierTopicInitLeader) {
            return append((TierTopicInitLeader) entry);
        } else if (entry instanceof TierObjectMetadata) {
            return append((TierObjectMetadata) entry);
        } else {
            throw new RuntimeException(String.format("Unknown AbstractTierMetadataType %s", entry));
        }
    }

    public String path() {
        throw new UnsupportedOperationException(
                "getPath not supported for memory tier partition status.");
    }

    public long numSegments() {
        return objectMetadataEntries.size();
    }

    public void flush() { }

    public void close() { }

    public void delete() { }

    public Optional<TierObjectMetadata> getObjectMetadataForOffset(long targetOffset) {
        return objectMetadataEntries
                .stream()
                .filter(m -> m.startOffset() >= targetOffset
                        || targetOffset > m.startOffset() && targetOffset < m.endOffset())
                .findFirst();
    }

    private AppendResult append(TierObjectMetadata objectMetadata) {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            OptionalLong endOffset = endOffset();
            if (!endOffset.isPresent()
                || objectMetadata.startOffset() > endOffset.getAsLong()) {
                objectMetadataEntries.add(objectMetadata);
                return AppendResult.ACCEPTED;
            }
        }
        return AppendResult.FENCED;
    }

    private AppendResult append(TierTopicInitLeader initLeader) {
        if (initLeader.tierEpoch() >= currentEpoch.get()) {
            currentEpoch.set(initLeader.tierEpoch());
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }

}
