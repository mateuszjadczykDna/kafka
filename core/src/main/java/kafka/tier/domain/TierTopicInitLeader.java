/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.serdes.InitLeader;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public class TierTopicInitLeader extends AbstractTierMetadata {
    public final static byte ID = 0;
    private final static byte VERSION_VO = 0;
    private final static byte CURRENT_VERSION = VERSION_VO;
    private final static int INITIAL_BUFFER_SIZE = 60;
    private final String topic;
    private final int partition;
    private final InitLeader init;

    public TierTopicInitLeader(String topic, int partition, int tierEpoch, UUID messageId, int brokerId) {
        if (tierEpoch < 0) {
            throw new IllegalArgumentException(String.format("Illegal tierEpoch supplied %d.", tierEpoch));
        }

        this.topic = topic;
        this.partition = partition;
        final FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE)
                .forceDefaults(true);
        InitLeader.startInitLeader(builder);
        int messageIdId = kafka.tier.serdes.UUID.createUUID(builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        InitLeader.addMessageId(builder, messageIdId);
        InitLeader.addTierEpoch(builder, tierEpoch);
        InitLeader.addBrokerId(builder, brokerId);
        InitLeader.addVersion(builder, CURRENT_VERSION);
        int entryId = InitLeader.endInitLeader(builder);
        builder.finish(entryId);
        this.init = InitLeader.getRootAsInitLeader(builder.dataBuffer());
    }

    public TierTopicInitLeader(String topic, int partition, InitLeader init) {
        this.topic = topic;
        this.partition = partition;
        this.init = init;
    }

    public byte type() {
        return ID;
    }

    public ByteBuffer payloadBuffer() {
        return init.getByteBuffer().duplicate();
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public int tierEpoch() {
        return init.tierEpoch();
    }

    public byte version() {
        return init.version();
    }

    public UUID messageId() {
        return new UUID(init.messageId().mostSignificantBits(), init.messageId().leastSignificantBits());
    }

    public int brokerId() {
        return init.brokerId();
    }

    public int hashCode() {
        return Objects.hash(topic, partition, version(), tierEpoch(), brokerId(), messageId());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TierTopicInitLeader that = (TierTopicInitLeader) o;
        return topic.equals(that.topic)
                && partition == that.partition
                && version() == that.version()
                && tierEpoch() == that.tierEpoch()
                && brokerId() == that.brokerId()
                && messageId().equals(that.messageId());
    }

    @Override
    public String toString() {
        return String.format(
                "TierInitLeader(topic='%s', partition=%s, tierEpoch=%s, "
                        + "magic=%s, messageId='%s', brokerId=%s)",
                topic, partition, tierEpoch(), version(), messageId(), brokerId());
    }
}
