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
            if (bf.position() + ENTRY_LENGTH_SIZE <= bf.limit()) {
                short length = bf.getShort(bf.position());
                if (bf.position() + length <= bf.limit()) {
                    return true;
                }

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
        // we must reread the length so that hasNext is idempotent
        short length = bf.getShort();
        final ObjectMetadata entry = ObjectMetadata.getRootAsObjectMetadata(bf);
        bf.position(bf.position() + length);
        return entry;
    }
}
