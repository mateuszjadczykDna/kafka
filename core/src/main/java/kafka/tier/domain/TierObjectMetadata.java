/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Objects;

public class TierObjectMetadata extends AbstractTierMetadata {
    public final static byte ID = 1;
    private final TopicPartition topicPartition;
    private final ObjectMetadata metadata;
    private final static byte VERSION_VO = 0;
    private final static byte CURRENT_VERSION = VERSION_VO;
    private final static int BASE_BUFFER_SIZE = 100;

    public TierObjectMetadata(TopicPartition topicPartition, ObjectMetadata metadata) {
        this.topicPartition = topicPartition;
        this.metadata = metadata;
    }

    public TierObjectMetadata(TopicPartition topicPartition, int tierEpoch,
                              long startOffset, int endOffsetDelta,
                              long lastStableOffset, long maxTimestamp,
                              long lastModifiedTime, int size,
                              boolean aborts, byte state) {
        if (tierEpoch < 0) {
            throw new IllegalArgumentException(String.format("Illegal tierEpoch supplied %d.", tierEpoch));
        }
        final FlatBufferBuilder builder = new FlatBufferBuilder(BASE_BUFFER_SIZE).forceDefaults(true);
        this.topicPartition = topicPartition;
        final int entryId = ObjectMetadata.createObjectMetadata(
                builder,
                tierEpoch,
                startOffset,
                endOffsetDelta,
                lastStableOffset,
                maxTimestamp,
                lastModifiedTime,
                size,
                aborts,
                CURRENT_VERSION,
                state);
        builder.finish(entryId);
        this.metadata = ObjectMetadata.getRootAsObjectMetadata(builder.dataBuffer());
    }

    public ObjectMetadata objectMetadata() {
        return metadata;
    }

    public byte type() {
        return ID;
    }

    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    public int tierEpoch() {
        return metadata.tierEpoch();
    }

    public long startOffset() {
        return metadata.startOffset();
    }

    public int endOffsetDelta() {
        return metadata.endOffsetDelta();
    }

    public long lastModifiedTime() {
        return metadata.lastModifiedTime();
    }

    public long endOffset() {
        return startOffset() + endOffsetDelta();
    }

    public long lastStableOffset() {
        return metadata.lastStableOffset();
    }

    public long maxTimestamp() {
        return metadata.maxTimestamp();
    }

    public int size() {
        return metadata.size();
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public boolean hasAborts() {
        return metadata.hasAborts();
    }

    public byte state() {
        return metadata.state();
    }

    public short version() {
        return metadata.version();
    }

    @Override
    public String toString() {
        return String.format("TierObjectMetadata(topic='%s', partition=%s,"
                        + " tierEpoch=%s, version=%s, startOffset=%s,"
                        + " endOffsetDelta=%s, lastStableOffset=%s, hasAborts=%s,"
                        + " maxTimestamp=%s, lastModifiedTime=%s, size=%s, status=%s)",
                topicPartition.topic(), topicPartition.partition(), tierEpoch(), version(), startOffset(),
                endOffsetDelta(), lastStableOffset(), hasAborts(), maxTimestamp(), lastModifiedTime(), size(), state());
    }

    public int hashCode() {
        return Objects.hash(topicPartition, tierEpoch(),
                startOffset(), endOffsetDelta(), lastStableOffset(),
                hasAborts(), maxTimestamp(), lastModifiedTime(), size(),
                version(), state());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TierObjectMetadata that = (TierObjectMetadata) o;
        return Objects.equals(topicPartition, that.topicPartition)
                && Objects.equals(tierEpoch(), that.tierEpoch())
                && Objects.equals(startOffset(), that.startOffset())
                && Objects.equals(endOffsetDelta(), that.endOffsetDelta())
                && Objects.equals(lastStableOffset(), that.lastStableOffset())
                && Objects.equals(hasAborts(), that.hasAborts())
                && Objects.equals(maxTimestamp(), that.maxTimestamp())
                && Objects.equals(lastModifiedTime(), that.lastModifiedTime())
                && Objects.equals(size(), that.size())
                && Objects.equals(version(), that.version())
                && Objects.equals(state(), that.state());
    }
}
