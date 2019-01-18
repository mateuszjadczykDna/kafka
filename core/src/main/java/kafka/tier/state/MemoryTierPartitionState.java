/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.Log$;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryTierPartitionState implements TierPartitionState {
    private final ConcurrentNavigableMap<Long, TierObjectMetadata> segmentMap = new ConcurrentSkipListMap<>();
    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final TopicPartition topicPartition;

    private File dir;
    private Object segmentMapLock = new Object();

    private volatile TierPartitionStatus status;
    private volatile boolean tieringEnabled;
    private volatile boolean closed = false;

    public MemoryTierPartitionState(File dir, TopicPartition topicPartition, boolean tieringEnabled) {
        this.dir = dir;
        this.status = TierPartitionStatus.CLOSED;
        this.topicPartition = topicPartition;
        this.tieringEnabled = tieringEnabled;
        maybeOpen();
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public File dir() {
        return dir;
    }

    @Override
    public OptionalLong endOffset() {
        Map.Entry<Long, TierObjectMetadata> lastEntry = segmentMap.lastEntry();
        if (lastEntry != null)
            return OptionalLong.of(metadata(lastEntry.getKey()).get().endOffset());
        return OptionalLong.empty();
    }

    @Override
    public OptionalLong startOffset() {
        Map.Entry<Long, TierObjectMetadata> firstEntry = segmentMap.firstEntry();
        if (firstEntry != null)
            return OptionalLong.of(firstEntry.getKey());
        return OptionalLong.empty();
    }

    @Override
    public TierPartitionStatus status() {
        return status;
    }

    @Override
    public void updateDir(File dir) {
        this.dir = dir;
    }

    @Override
    public long totalSize() {
        long size = 0;
        for (TierObjectMetadata tierObjectMetadata : segmentMap.values())
            size += tierObjectMetadata.size();
        return size;
    }

    @Override
    public int tierEpoch() {
        return currentEpoch.get();
    }

    @Override
    public boolean tieringEnabled() {
        return tieringEnabled;
    }

    @Override
    public void onTieringEnable() throws IOException {
        tieringEnabled = true;
        maybeOpen();
    }

    @Override
    public TierPartitionState.AppendResult append(AbstractTierMetadata entry) {
        if (!status.isOpenForWrite() || closed) {
            return AppendResult.ILLEGAL;
        } else if (entry instanceof TierTopicInitLeader) {
            return append((TierTopicInitLeader) entry);
        } else if (entry instanceof TierObjectMetadata) {
            return append((TierObjectMetadata) entry);
        } else {
            throw new RuntimeException(String.format("Unknown AbstractTierMetadataType %s", entry));
        }
    }

    @Override
    public String path() {
        return dir.getAbsolutePath();
    }

    @Override
    public int numSegments() {
        return segmentMap.size();
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        return segmentMap.keySet();
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        return Log$.MODULE$.logSegments(segmentMap, from, to, segmentMapLock).keySet();
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) {
        Map.Entry<Long, TierObjectMetadata> entry = segmentMap.floorEntry(targetOffset);
        if (entry != null)
            return Optional.of(entry.getValue());
        else
            return Optional.empty();
    }

    public void flush() {
    }

    @Override
    public void beginCatchup() {
        if (!tieringEnabled)
            throw new IllegalStateException("Illegal state for tier partition state");
        maybeOpen();
        status = TierPartitionStatus.CATCHUP;
    }

    @Override
    public void onCatchUpComplete() {
        if (!tieringEnabled)
            throw new IllegalStateException("Illegal state for tier partition state");
        maybeOpen();
        status = TierPartitionStatus.ONLINE;
    }

    public void close() {
        synchronized (segmentMapLock) {
            segmentMap.clear();
            closed = true;
        }
    }

    @Override
    public void closeHandlers() {
        close();
    }

    public void delete() {
        close();
    }

    private void maybeOpen() {
        if (tieringEnabled)
            status = TierPartitionStatus.READ_ONLY;
    }

    private AppendResult append(TierObjectMetadata objectMetadata) {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            OptionalLong endOffset = endOffset();
            if (!endOffset.isPresent() || objectMetadata.startOffset() > endOffset.getAsLong()) {
                segmentMap.put(objectMetadata.startOffset(), objectMetadata);
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
