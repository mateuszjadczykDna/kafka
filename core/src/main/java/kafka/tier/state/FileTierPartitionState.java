/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.Log;
import kafka.log.Log$;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierPartitionStateIllegalListenerException;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class FileTierPartitionState implements TierPartitionState, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final long FILE_OFFSET = 0;

    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final TopicPartition topicPartition;
    private final Object lock = new Object();
    // Replica Fetcher needs to track materialization progress in order
    // to restore tier state aligned with local data available on the leader
    private ReplicationMaterializationListener materializationTracker = null;

    private File dir;
    private String path;
    private ConcurrentNavigableMap<Long, Long> segments = new ConcurrentSkipListMap<>();
    private FileChannel channel;
    private Long endOffset = null;

    private volatile boolean tieringEnabled;
    private volatile TierPartitionStatus status;

    public FileTierPartitionState(File dir, TopicPartition topicPartition, boolean tieringEnabled) throws IOException {
        this.topicPartition = topicPartition;
        this.dir = dir;
        this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
        this.status = TierPartitionStatus.CLOSED;
        this.tieringEnabled = tieringEnabled;
        maybeOpenFile();
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean tieringEnabled() {
        return tieringEnabled;
    }

    @Override
    public void onTieringEnable() throws IOException {
        synchronized (lock) {
            this.tieringEnabled = true;
            maybeOpenFile();
        }
    }

    @Override
    public Optional<Long> startOffset() {
        Map.Entry<Long, Long> firstEntry = segments.firstEntry();
        if (firstEntry != null)
            return Optional.of(firstEntry.getKey());
        return Optional.empty();
    }

    @Override
    public Optional<Long> endOffset() {
        return Optional.ofNullable(endOffset);
    }

    @Override
    public long totalSize() throws IOException {
        long size = 0;
        Map.Entry<Long, Long> firstEntry = segments.firstEntry();

        if (firstEntry != null) {
            FileTierPartitionIterator iterator = iterator(firstEntry.getValue());
            while (iterator.hasNext())
                size += iterator.next().size();
        }
        return size;
    }

    @Override
    public void flush() throws IOException {
        synchronized (lock) {
            if (status.isOpenForWrite())
                channel.force(true);
        }
    }

    @Override
    public int tierEpoch() {
        return currentEpoch.get();
    }

    @Override
    public File dir() {
        return dir;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public void delete() throws IOException {
        synchronized (lock) {
            segments.clear();
            closeHandlers();
            Files.deleteIfExists(Paths.get(path));
        }
    }

    @Override
    public void updateDir(File dir) {
        synchronized (lock) {
            this.path = Log.tierStateFile(dir, FILE_OFFSET, "").getAbsolutePath();
            this.dir = dir;
        }
    }

    @Override
    public void closeHandlers() throws IOException {
        synchronized (lock) {
            if (status != TierPartitionStatus.CLOSED) {
                try {
                    if (channel != null)
                        channel.close();
                } finally {
                    channel = null;
                    segments.clear();

                    if (materializationTracker != null) {
                        materializationTracker.promise.completeExceptionally(new TierPartitionStateIllegalListenerException(
                                "Tier partition state for " + topicPartition + " has been closed. "
                                + "Materialization tracking canceled."));
                    }
                    status = TierPartitionStatus.CLOSED;
                }
            }
        }
    }

    @Override
    public TierPartitionStatus status() {
        return status;
    }

    @Override
    public void beginCatchup() {
        synchronized (lock) {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);
            status = TierPartitionStatus.CATCHUP;
        }
    }

    @Override
    public void onCatchUpComplete() {
        synchronized (lock) {
            if (!tieringEnabled || !status.isOpen())
                throw new IllegalStateException("Illegal state " + status + " for tier partition. " +
                        "tieringEnabled: " + tieringEnabled + " file: " + path);
            status = TierPartitionStatus.ONLINE;
        }
    }

    @Override
    public int numSegments() {
        return segments.size();
    }

    @Override
    public Future<TierObjectMetadata> materializationListener(long targetOffset) throws IOException {
        final CompletableFuture<TierObjectMetadata> promise = new CompletableFuture<>();
        synchronized (lock) {
            if (!status.isOpen()) {
                promise.completeExceptionally(new TierPartitionStateIllegalListenerException("Tier "
                        + "partition state for " + topicPartition + " is not open. "
                        + "Materialization tracker could not be created."));
                return promise;
            }

            final Optional<TierObjectMetadata> metadata = metadata(targetOffset);
            // listener is able to fire immediately
            if (metadata.isPresent() && metadata.get().endOffset() >= targetOffset) {
                promise.complete(metadata.get());
            } else {
                if (materializationTracker != null)
                    materializationTracker.promise.completeExceptionally(
                            new IllegalStateException("Cancelled materialization tracker, as "
                                    + "another materialization tracker has been started."));

                materializationTracker = new ReplicationMaterializationListener(targetOffset, promise);
            }
        }
        return promise;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            try {
                flush();
            } finally {
                closeHandlers();
            }
        }
    }

    public AppendResult append(AbstractTierMetadata entry) throws IOException {
        synchronized (lock) {
            if (!status.isOpenForWrite()) {
                return AppendResult.ILLEGAL;
            } else if (entry instanceof TierTopicInitLeader) {
                return append((TierTopicInitLeader) entry);
            } else if (entry instanceof TierObjectMetadata) {
                return append((TierObjectMetadata) entry);
            } else {
                throw new RuntimeException(String.format("Unknown TierTopicIndexEntryType %s", entry));
            }
        }
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        return segments.keySet();
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        return Log$.MODULE$.logSegments(segments, from, to, lock).keySet();
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException {
        Map.Entry<Long, Long> entry = segments.floorEntry(targetOffset);
        if (entry != null) {
            return read(entry.getValue());
        } else {
            return Optional.empty();
        }
    }

    public FileTierPartitionIterator iterator(long position) throws IOException {
        return new FileTierPartitionIterator(topicPartition, channel, position);
    }

    private void maybeOpenFile() throws IOException {
        if (tieringEnabled && !status.isOpen()) {
            channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            scanAndTruncate();
            channel.position(channel.size());
            status = TierPartitionStatus.READ_ONLY;
        }
    }

    private Optional<TierObjectMetadata> read(long position) throws IOException {
        if (!segments.isEmpty() && position < channel.size()) {
            FileTierPartitionIterator iterator = iterator(position);
            // The entry at `position` must be known to be fully written to the underlying file
            if (!iterator.hasNext())
                throw new IllegalStateException("Could not read entry at " + position + " for partition " + topicPartition);
            return Optional.of(iterator.next());
        }
        return Optional.empty();
    }

    /**
     * Return internal FileChannel.
     * For testing use only.
     */
    FileChannel channel() {
        return channel;
    }

    private AppendResult append(TierTopicInitLeader initLeader) {
        int epoch = currentEpoch.get();
        if (initLeader.tierEpoch() >= epoch) {
            currentEpoch.set(initLeader.tierEpoch());
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }

    private AppendResult append(TierObjectMetadata objectMetadata) throws IOException {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            if (!endOffset().isPresent() || objectMetadata.endOffset() > endOffset) {
                final ByteBuffer metadataBuffer = objectMetadata.payloadBuffer();
                final long byteOffset = appendWithSizePrefix(channel, metadataBuffer);
                addSegment(objectMetadata.objectMetadata(), byteOffset);

                if (status.isOpen()
                        && materializationTracker != null
                        && objectMetadata.endOffset() >= materializationTracker.offsetToMaterialize) {
                    materializationTracker.promise.complete(objectMetadata);
                    materializationTracker = null;
                }

                return AppendResult.ACCEPTED;
            }
        }
        return AppendResult.FENCED;
    }

    private static long appendWithSizePrefix(FileChannel channel, ByteBuffer metadataBuffer) throws IOException {
        final long byteOffset = channel.position();
        final short sizePrefix = (short) metadataBuffer.remaining();
        final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(0, sizePrefix);
        Utils.writeFully(channel, sizeBuf);
        Utils.writeFully(channel, metadataBuffer);
        return byteOffset;
    }

    private void scanAndTruncate() throws IOException {
        FileTierPartitionIterator iterator = iterator(0);
        long currentPosition = 0;

        while (iterator.hasNext()) {
            TierObjectMetadata metadata = iterator.next();

            // epoch must not go backwards
            if (metadata.tierEpoch() < currentEpoch.get())
                throw new IllegalStateException("Read unexpected epoch " + metadata.tierEpoch() + " currentEpoch: " +
                        currentEpoch + " position: " + currentPosition + "topicPartition: " + topicPartition);

            // set the epoch
            currentEpoch.set(metadata.tierEpoch());
            addSegment(metadata.objectMetadata(), currentPosition);

            // advance position
            currentPosition = iterator.position();
        }

        if (currentPosition < channel.size()) {
            log.debug("Truncating to {}/{} for partition {}", currentPosition, channel.size(), topicPartition);
            channel.truncate(currentPosition);
        }
    }

    private void addSegment(ObjectMetadata metadata, long byteOffset) {
        // As there may be arbitrary overlap between segments, it is possible for a new
        // segment to completely overlap a previous segment. We rely on on lookup via the
        // start offset, and if we insert into the lookup map with the raw offset, it is possible
        // for portions of a segment to be unfetchable unless we bound overlapping segments
        // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
        // at 50, the portion 201 - 250 will be inaccessible.
        segments.put(Math.max(endOffset().orElse(-1L) + 1, metadata.startOffset()),
                byteOffset);
        // store end offset for immediate access
        endOffset = metadata.startOffset() + metadata.endOffsetDelta();
    }

    private static class ReplicationMaterializationListener {
        final CompletableFuture<TierObjectMetadata> promise;
        final long offsetToMaterialize;

        ReplicationMaterializationListener(long offsetToMaterialize,
                                           CompletableFuture<TierObjectMetadata> promise) {
            this.offsetToMaterialize = offsetToMaterialize;
            this.promise = promise;
        }
    }
}
