/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.serdes.ObjectMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Iterator;

class FileTierPartitionIterator implements Iterator<ObjectMetadata> {
    private long channelPosition;
    private final ByteBuffer bf;
    private FileChannel channel;
    private ObjectMetadata entry;
    private short entryLength;
    private static final int ENTRY_LENGTH_SIZE = 2;
    private static final int BUFFER_SIZE = 4096;

    FileTierPartitionIterator(FileChannel channel, long startPosition) throws java.io.IOException {
        this.channel = channel;
        bf = ByteBuffer.allocate(BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        channelPosition = startPosition + channel.read(bf, startPosition);
        bf.flip();
    }

    public boolean hasNext() {
        try {
            if (entry != null) {
                return true;
            } else if (bf.position() + ENTRY_LENGTH_SIZE <= bf.limit()) {
                bf.mark();
                entryLength = bf.getShort();
                if (bf.position() + entryLength <= bf.limit()) {
                    entry = ObjectMetadata.getRootAsObjectMetadata(bf);
                    return true;
                }

                // reset position as we couldn't perform a full read
                bf.reset();
                final int read = FileUtils.reloadBuffer(channel, bf, channelPosition);
                if (read <= 0) {
                    return false;
                } else {
                    channelPosition += read;
                    return hasNext();
                }
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return false;
    }

    @Override
    public ObjectMetadata next() {
        if (entry == null) {
            throw new IllegalStateException("hasNext must be called prior to calling next.");
        }
        bf.position(bf.position() + entryLength);
        ObjectMetadata returnEntry = entry;
        entry = null;
        return returnEntry;
    }
}
