package kafka.tier.state;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtils {
    /**
     * Loads data from a FileChannel into a ByteBuffer.
     *
     * @param channel         the FileChannel for the TierPartitionState file
     * @param bf              ByteBuffer to load data into.
     * @param channelPosition the position in the FileChannel
     * @return the number of bytes read.
     * @throws IOException
     */
    public static int reloadBuffer(FileChannel channel,
                                   ByteBuffer bf,
                                   long channelPosition) throws IOException {
        // move overread to the beginning of the buffer
        final byte[] remaining = new byte[bf.remaining()];
        bf.get(remaining);
        bf.flip();
        bf.put(remaining);
        int read = channel.read(bf, channelPosition);
        bf.flip();
        return read;
    }
}
