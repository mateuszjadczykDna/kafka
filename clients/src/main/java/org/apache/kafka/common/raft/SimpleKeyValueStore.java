package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * This is an experimental key-value store built on top of Raft consensus. Really
 * just trying to figure out what a useful API looks like.
 */
public class SimpleKeyValueStore<K, V> {
    private final RaftManager raftManager;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Map<K, V> map = new HashMap<>();
    private OffsetAndEpoch currentPosition = new OffsetAndEpoch(0L, 0);
    private SortedMap<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>> pendingCommit = new TreeMap<>();

    public SimpleKeyValueStore(RaftManager raftManager,
                               Serde<K> keySerde,
                               Serde<V> valueSerde) {
        this.raftManager = raftManager;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized CompletableFuture<OffsetAndEpoch> put(K key, V value) {
        return putAll(Collections.singletonMap(key, value));
    }

    public synchronized CompletableFuture<OffsetAndEpoch> putAll(Map<K, V> map) {
        // Append returns after the data was accepted by the leader, but we need to wait
        // for it to be committed.
        CompletableFuture<OffsetAndEpoch> appendFuture = raftManager.append(buildRecords(map));
        return appendFuture.thenComposeAsync(offsetAndEpoch -> {
            CompletableFuture<OffsetAndEpoch> commitFuture = new CompletableFuture<>();
            pendingCommit.put(offsetAndEpoch, commitFuture);
            return commitFuture;
        });
    }

    private Records buildRecords(Map<K, V> map) {
        SimpleRecord[] records = new SimpleRecord[map.size()];
        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            records[i++] = serialize(entry.getKey(), entry.getValue());
        }
        return MemoryRecords.withRecords(CompressionType.NONE, records);
    }

    private SimpleRecord serialize(K key, V value) {
        byte[] keyBytes = keySerde.serializer().serialize(null, key);
        byte[] valueBytes = valueSerde.serializer().serialize(null, value);
        return new SimpleRecord(keyBytes, valueBytes);
    }

    private void putRecords(Records records) {
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                byte[] keyBytes = Utils.toArray(record.key());
                byte[] valueBytes = Utils.toArray(record.value());
                K key = keySerde.deserializer().deserialize(null, keyBytes);
                V value = valueSerde.deserializer().deserialize(null, valueBytes);
                map.put(key, value);
            }
            maybeCompletePendingCommit(batch);
            currentPosition = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    private void maybeCompletePendingCommit(RecordBatch batch) {
        Iterator<Map.Entry<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>>> iter =
                pendingCommit.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>> entry = iter.next();
            CompletableFuture<OffsetAndEpoch> future = entry.getValue();
            OffsetAndEpoch offsetAndEpoch = entry.getKey();
            if (future.isCompletedExceptionally() || future.isCancelled()) {
                iter.remove();
            } else if (batch.partitionLeaderEpoch() > offsetAndEpoch.epoch) {
                future.completeExceptionally(new LogTruncationException());
                iter.remove();
            } else if (batch.lastOffset() >= offsetAndEpoch.offset) {
                future.complete(offsetAndEpoch);
                iter.remove();
            } else {
                break;
            }
        }
    }

    public synchronized void sync() {
        Records records = raftManager.read(currentPosition);
        putRecords(records);
    }

}
