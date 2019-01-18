/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import kafka.tier.serdes.InitLeader;
import kafka.tier.serdes.ObjectMetadata;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;

public abstract class AbstractTierMetadata {
    private static final Logger log = LoggerFactory.getLogger(AbstractTierMetadata.class);
    private static final int TOPIC_LENGTH_LENGTH = 2;
    private static final int PARTITION_LENGTH = 4;
    private static final int TYPE_LENGTH = 1;

    public byte[] serializeKey() {
        byte[] topicBytes = Utils.utf8(topicPartition().topic());
        final ByteBuffer buf = ByteBuffer.allocate(
                TOPIC_LENGTH_LENGTH + topicBytes.length + PARTITION_LENGTH);
        buf.putShort((short) topicPartition().topic().length());
        buf.put(topicBytes);
        buf.putInt(topicPartition().partition());
        return buf.array();
    }

    public byte[] serializeValue() {
        final ByteBuffer payload = payloadBuffer();
        final ByteBuffer buf = ByteBuffer.allocate(payload.remaining() + TYPE_LENGTH);
        buf.put(type());
        buf.put(payload);
        return buf.array();
    }

    /**
     * Deserializes byte key and value read from Tier Topic into Tier Metadata.
     * @param key Key containing archived topic partition
     * @param value Value containing tier metadata.
     * @return AbstractTierMetadata if one could be deserialized. Empty if Tier Metadata ID unrecognized.
     * @throws TierMetadataDeserializationException
     */
    public static Optional<AbstractTierMetadata> deserialize(byte[] key, byte[] value)
            throws TierMetadataDeserializationException {
        final ByteBuffer keyBuf = ByteBuffer.wrap(key);
        final ByteBuffer valueBuf = ByteBuffer.wrap(value);

        // deserialize key for topic and partition
        final int topicStrLen = keyBuf.getShort();
        final byte[] topicStrBuf = ByteBuffer.allocate(topicStrLen).array();
        keyBuf.get(topicStrBuf);
        final String topic = Utils.utf8(topicStrBuf);
        final int partition = keyBuf.getInt();
        final TopicPartition topicPartition = new TopicPartition(topic, partition);

        // deserialize value header with record type and tierEpoch
        final byte type = valueBuf.get();
        switch (type) {
            case TierTopicInitLeader.ID:
                final InitLeader init = InitLeader.getRootAsInitLeader(valueBuf);
                return Optional.of(new TierTopicInitLeader(topicPartition, init));
            case TierObjectMetadata.ID:
                final ObjectMetadata metadata = ObjectMetadata.getRootAsObjectMetadata(valueBuf);
                return Optional.of(new TierObjectMetadata(topicPartition, metadata));
            default:
                log.debug("Unknown tier metadata type with ID {}. Ignoring record.", type);
                return Optional.empty();
        }
    }

    /**
     * @return byte ID for this metadata entry type.
     */
    public abstract byte type();

    /**
     * Topic-partition corresponding to this tier metadata.
     * @return topic partition
     */
    public abstract TopicPartition topicPartition();

    /**
     * tierEpoch for the tier metadata
     * @return tierEpoch
     */
    public abstract int tierEpoch();

    /**
     * @return backing payload buffer for this metadata.
     */
    public abstract ByteBuffer payloadBuffer();
}
