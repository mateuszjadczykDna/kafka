/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FileTierPartitionState implements TierPartitionState, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionState.class);
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final int READ_BUFFER_SIZE = 4096;
    private final String path;
    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final String topic;
    private final int partition;
    private final SparsePositionMap positions;
    private volatile TierPartitionStatus status;
    private FileChannel channel;
    private Long endOffset = null;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public FileTierPartitionState(String baseDir,
                                  TopicPartition topicPartition,
                                  double positionIndexSparsity) {
        this.status = TierPartitionStatus.INIT;
        this.topic = topicPartition.topic();
        this.partition = topicPartition.partition();
        this.path = String.format("%s/%s_%s.tierlog",
                baseDir,
                topicPartition.topic(),
                topicPartition.partition());
        positions = new SparsePositionMap(positionIndexSparsity);
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public OptionalLong beginningOffset() throws java.io.IOException {
        Iterator<ObjectMetadata> iterator = iterator();
        if (iterator.hasNext()) {
            return OptionalLong.of(iterator.next().startOffset());
        } else {
            return OptionalLong.empty();
        }
    }

    public OptionalLong endOffset() {
        if (endOffset == null) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(endOffset);
        }
    }

    public long totalSize() throws java.io.IOException {
        long size = 0;
        Iterator<ObjectMetadata> iterator = iterator();
        while (iterator.hasNext()) {
            ObjectMetadata e = iterator.next();
            size += e.size();
        }
        return size;
    }

    public void flush() throws IOException {
        if (status != TierPartitionStatus.INIT) {
            channel.force(true);
        }
    }

    public int tierEpoch() {
        return currentEpoch.get();
    }

    public String path() {
        return path;
    }

    public void delete() {
        if (status != TierPartitionStatus.INIT) {
            try {
                Files.delete(Paths.get(path));
            } catch (IOException ioe) {
                log.warn("Exception deleting tier partition status.", ioe);
            }
        }
    }

    public TierPartitionStatus status() {
        return status;
    }

    public void targetStatus(TierPartitionStatus state) throws IOException {
        if (this.status != TierPartitionStatus.INIT && state == TierPartitionStatus.CATCHUP) {
            throw new IllegalStateException();
        }
        if (this.status == TierPartitionStatus.INIT && state == TierPartitionStatus.CATCHUP) {
            channel = FileChannel.open(Paths.get(path),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            scanAndTruncate();
            channel.position(channel.size());
        }
        assert state != TierPartitionStatus.ONLINE || channel != null;
        this.status = state;
    }

    public long numSegments() throws IOException {
        long count = 0;
        Iterator<ObjectMetadata> iterator = iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            flush();
            if (channel != null) {
                channel.close();
            }
        }
    }

    public AppendResult append(AbstractTierMetadata entry) throws java.io.IOException {
        if (status == TierPartitionStatus.INIT) {
            return AppendResult.ILLEGAL;
        } else if (entry instanceof TierTopicInitLeader) {
            return append((TierTopicInitLeader) entry);
        } else if (entry instanceof TierObjectMetadata) {
            return append((TierObjectMetadata) entry);
        } else {
            throw new RuntimeException(String.format("Unknown TierTopicIndexEntryType %s", entry));
        }
    }

    public Iterator<ObjectMetadata> iterator() throws java.io.IOException {
        if (status == TierPartitionStatus.INIT) {
            return new ArrayList<ObjectMetadata>().iterator();
        } else {
            return new FileTierPartitionIterator(channel, 0);
        }
    }

    public Optional<TierObjectMetadata> getObjectMetadataForOffset(long targetOffset)
            throws IOException {
        ByteBuffer bf = ByteBuffer.allocate(READ_BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        long channelPosition = positions.floorPosition(targetOffset).orElse(0L);

        channelPosition += channel.read(bf, channelPosition);
        bf.flip();
        while (bf.limit() > 0) {
            short entryLength = getMetadataLength(bf);
            while (entryLength > 0) {
                final ObjectMetadata entry = ObjectMetadata.getRootAsObjectMetadata(bf);
                if (entry.startOffset() >= targetOffset
                        || (targetOffset >= entry.startOffset()
                        && targetOffset < entry.startOffset() + entry.endOffsetDelta())) {
                    // returning entry without copying entry bytes directly will cause
                    // READ_BUFFER_SIZE memory usage until GC can occur.
                    // We may want to copy the buffer if this is a problem.
                    return Optional.of(new TierObjectMetadata(topic, partition, entry));
                }
                bf.position(bf.position() + entryLength);
                entryLength = getMetadataLength(bf);
            }
            final int read = FileUtils.reloadBuffer(channel, bf, channelPosition);
            if (read == -1) {
                return Optional.empty();
            }
            channelPosition += read;
        }

        return Optional.empty();
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

    private AppendResult append(TierObjectMetadata m) throws java.io.IOException {
        if (m.tierEpoch() == tierEpoch()) {
            OptionalLong endOffset = endOffset();
            if (!endOffset.isPresent() || m.startOffset() > endOffset.getAsLong()) {
                final ByteBuffer buffer = m.payloadBuffer();
                final long byteOffset = appendWithSizePrefix(channel, buffer);
                updateEndPosition(m.objectMetadata(), byteOffset);
                return AppendResult.ACCEPTED;
            }
        }
        return AppendResult.FENCED;
    }


    private static long appendWithSizePrefix(FileChannel channel,
                                             ByteBuffer buffer)
            throws IOException {
        final long byteOffset = channel.position();
        final short sizePrefix = (short) buffer.remaining();
        final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putShort(0, sizePrefix);
        final int sizeWritten = channel.write(sizeBuf);
        assert sizeWritten != 0;
        final int written = channel.write(buffer);
        assert written != 0;
        return byteOffset;
    }

    private short getMetadataLength(ByteBuffer bf) {
        if (bf.position() + ENTRY_LENGTH_SIZE <= bf.limit()) {
            bf.mark();
            final short length = bf.getShort();
            if (bf.position() + length <= bf.limit()) {
                // sufficient capacity to read the entry
                return length;
            }
            bf.reset();
        }
        return -1;
    }

    private void scanAndTruncate() throws IOException {
        final ByteBuffer sizeBuf = ByteBuffer.allocate(ENTRY_LENGTH_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        long position = 0;
        int read = channel.read(sizeBuf, position);
        while (read == ENTRY_LENGTH_SIZE) {
            sizeBuf.flip();
            short entrySize = sizeBuf.getShort();
            sizeBuf.flip();

            // read metadata entry
            assert entrySize > 0;
            final ByteBuffer entryBuf = ByteBuffer.allocate(entrySize)
                    .order(ByteOrder.LITTLE_ENDIAN);
            int entryRead = channel.read(entryBuf, position + ENTRY_LENGTH_SIZE);
            entryBuf.flip();

            if (entryRead != entrySize) {
                channel.truncate(position);
                break;
            }
            ObjectMetadata metadata = ObjectMetadata.getRootAsObjectMetadata(entryBuf);

            // tier partition status epoch is known to be good
            currentEpoch.set(metadata.tierEpoch());
            updateEndPosition(metadata, position);

            position = position + ENTRY_LENGTH_SIZE + entrySize;
            read = channel.read(sizeBuf, position);
        }

        if (position < channel.size()) {
            channel.truncate(position);
        }
    }

    private void updateEndPosition(ObjectMetadata metadata, long byteOffset) {
        // store end offset for immediate access
        this.endOffset = metadata.startOffset() + metadata.endOffsetDelta();

        // add to quick offset lookup map
        positions.add(metadata.startOffset(), byteOffset);
    }

    private static class SparsePositionMap {
        ConcurrentSkipListMap<Long, Long> positions;
        private long frequency;
        private long count = 0;

        SparsePositionMap(Double sparsity) {
            frequency = (long) (1.0 / sparsity);
        }

        public void add(Long offset, Long position) {
            if (count % frequency == 0) {
                // lazily setup skip list, as small partitions won't require a sparse map at all
                if (positions == null) {
                    positions = new ConcurrentSkipListMap<>();
                }

                positions.put(offset, position);
            }
            count++;
        }

        Optional<Long> floorPosition(Long offset) {
            if (positions == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(positions.floorEntry(offset)).map(Map.Entry::getValue);
        }
    }
}
