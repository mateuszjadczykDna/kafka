package org.apache.kafka.common.raft;

import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the simplest interesting state machine. It maintains a simple counter which can only
 * be incremented by one.
 */
public class DistributedCounter implements DistributedStateMachine {
    private final Logger log;
    private final RaftManager manager;
    private final AtomicInteger committed = new AtomicInteger(0);
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);
    private AtomicInteger uncommitted;

    public DistributedCounter(RaftManager manager, LogContext logContext) {
        manager.initialize(this);
        this.manager = manager;
        this.log = logContext.logger(DistributedCounter.class);
    }

    @Override
    public synchronized void becomeLeader(int epoch) {
        uncommitted = new AtomicInteger(committed.get());
    }

    @Override
    public synchronized void becomeFollower(int epoch) {
        uncommitted = null;
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                int value = deserialize(record);
                if (value != committed.get() + 1)
                    throw new IllegalStateException("Detected invalid increment in record at offset " + record.offset() +
                            ", epoch " + batch.partitionLeaderEpoch() + ": " + committed.get() + " -> " + value);
                log.trace("Applied counter update at offset {}: {} -> {}", record.offset(), committed.get(), value);
                committed.set(value);
            }
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }

        if (uncommitted != null && committed.get() > uncommitted.get())
            uncommitted.set(committed.get());
    }

    @Override
    public synchronized boolean accept(Records records) {
        int lastUncommitted = uncommitted.get();
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                int value = deserialize(record);
                if (value != lastUncommitted + 1)
                    return false;
                lastUncommitted = value;
            }
        }

        log.trace("Accept counter update: {} -> {}", uncommitted.get(), lastUncommitted);
        uncommitted.set(lastUncommitted);
        return true;
    }

    public synchronized int value() {
        return committed.get();
    }

    public synchronized CompletableFuture<Integer> increment() {
        int incremented = value() + 1;
        Records records = MemoryRecords.withRecords(CompressionType.NONE, serialize(incremented));
        CompletableFuture<OffsetAndEpoch> future = manager.append(records);
        return future.thenApply(offsetAndEpoch -> incremented);
    }

    private SimpleRecord serialize(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.INT32.write(buffer, value);
        buffer.flip();
        return new SimpleRecord(buffer);
    }

    private int deserialize(Record record) {
        return (Integer) Type.INT32.read(record.value());
    }

}
