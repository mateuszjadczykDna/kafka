package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.nio.ByteBuffer;

public class RaftUtil {

    public static byte[] serializeRecords(Records records) {
        // TODO: Add support for FileRecords
        if (!(records instanceof MemoryRecords)) {
            throw new UnsupportedOperationException("Serialization not yet supported for " + records.getClass());
        }

        MemoryRecords memoryRecords = (MemoryRecords) records;
        byte[] recordsBytes = new byte[records.sizeInBytes()];
        ByteBuffer buffer = ByteBuffer.wrap(recordsBytes);
        buffer.limit(recordsBytes.length);
        buffer.put(memoryRecords.buffer().duplicate());
        return recordsBytes;
    }

}
