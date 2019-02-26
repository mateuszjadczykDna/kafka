package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SimpleKeyValueStore<K, V> {
    private final RaftManager raftManager;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Map<K, V> map = new HashMap<>();
    private long currentOffset;

    public SimpleKeyValueStore(RaftManager raftManager,
                               Serde<K> keySerde,
                               Serde<V> valueSerde) {
        this.raftManager = raftManager;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public V get(K key) {
        return map.get(key);
    }

    public void put(K key, V value) {
        putAll(Collections.singletonMap(key, value));
    }

    public void putAll(Map<K, V> map) {

    }

    private Records buildRecords(Map<K, V> map) {
        SimpleRecord[] records = new SimpleRecord[map.size()];
        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            byte[] key = keySerde.serializer().serialize(null, entry.getKey());
            byte[] value = valueSerde.serializer().serialize(null, entry.getValue());
            records[i] = new SimpleRecord(key, value);
            i++;
        }
        return MemoryRecords.withRecords(CompressionType.NONE, records);
    }


    public void onHighWatermarkBumped(long highWatermark) {

    }

}
