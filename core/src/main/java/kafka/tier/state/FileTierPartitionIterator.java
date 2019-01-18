/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.serdes.ObjectMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

class FileTierPartitionIterator extends AbstractIterator<TierObjectMetadata> {
    private static final Logger log = LoggerFactory.getLogger(FileTierPartitionIterator.class);
    private static final int ENTRY_LENGTH_SIZE = 2;

    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(ENTRY_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    private final TopicPartition topicPartition;

    private long position;
    private long endPosition;
    private FileChannel channel;
    private ByteBuffer entryBuffer = null;

    public FileTierPartitionIterator(TopicPartition topicPartition,
                                     FileChannel channel,
                                     long startPosition) throws IOException {
        this.channel = channel;
        this.position = startPosition;
        this.endPosition = channel.size();
        this.topicPartition = topicPartition;
    }

    @Override
    protected TierObjectMetadata makeNext() {
        if (position >= endPosition)
            return allDone();

        try {
            long currentPosition = this.position;
            // read length
            Utils.readFully(channel, lengthBuffer, currentPosition);
            if (lengthBuffer.hasRemaining())
                return allDone();
            currentPosition += lengthBuffer.limit();
            lengthBuffer.flip();

            short length = lengthBuffer.getShort();

            // check if we have enough bytes to read the entry
            if (currentPosition + length > endPosition)
                return allDone();

            // reallocate entry buffer if needed
            if (entryBuffer == null || length > entryBuffer.capacity()) {
                log.debug("Allocating buffer of size " + length);
                entryBuffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
            }

            // read and return the entry
            entryBuffer.clear();
            entryBuffer.limit(length);
            Utils.readFully(channel, entryBuffer, currentPosition);
            if (entryBuffer.hasRemaining())
                return allDone();
            currentPosition += entryBuffer.limit();
            entryBuffer.flip();

            // advance position
            position = currentPosition;

            return new TierObjectMetadata(topicPartition, ObjectMetadata.getRootAsObjectMetadata(entryBuffer));
        } catch (IOException e) {
            throw new KafkaStorageException(e);
        }
    }

    public long position() {
        return position;
    }
}
