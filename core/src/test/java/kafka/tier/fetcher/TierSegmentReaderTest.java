/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TierSegmentReaderTest {
    @Test
    public void homogenousRecordBatchTest() {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };
        ByteBuffer records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE,
                TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer records2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 3L,
                CompressionType.NONE, TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(records.limit() + records2.limit());
        combinedBuffer.put(records);
        combinedBuffer.put(records2);
        combinedBuffer.flip();

        testExpected(combinedBuffer, 0L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 1L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 2L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 3L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 4L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 5L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 6L, Long.MAX_VALUE, null, null);
        testExpected(combinedBuffer, 7L, Long.MAX_VALUE, null, null);
        testExpected(combinedBuffer, 0L, 3L, 0L, 2L);
        testExpected(combinedBuffer, 3L, 4L, null, null);
        testExpected(combinedBuffer, 4L, 5L, null, null);
    }

    private void testExpected(ByteBuffer combinedBuffer, Long target, Long maxOffset,
                              Long expectedStart, Long expectedEnd) {
        combinedBuffer.position(0);
        try {
            ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
            CancellationContext cancellationContext = CancellationContext.newContext();
            MemoryRecords records =
                    TierSegmentReader.loadRecords(cancellationContext.subContext(), is, 1000, maxOffset, target);

            Long firstOffset = null;
            Long lastOffset = null;
            if (records.sizeInBytes() != 0) {
                Iterator<MutableRecordBatch> iterator = records.batches().iterator();
                while (iterator.hasNext()) {
                    MutableRecordBatch batch = iterator.next();
                    if (firstOffset == null) {
                        firstOffset = batch.baseOffset();
                    }
                    lastOffset = batch.lastOffset();
                }
            }
            assertEquals(expectedStart, firstOffset);
            assertEquals(expectedEnd, lastOffset);
        } catch (IOException ioe) {
            fail("Exception should not be thrown: " + ioe);
        }
    }
}
