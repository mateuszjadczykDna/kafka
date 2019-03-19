package org.apache.kafka.common.raft;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleKeyValueStoreTest {

    private RaftManager setupSingleNodeRaftManager() {
        int localId = 1;
        int electionTimeoutMs = 10000;
        int electionJitterMs = 50;
        int retryBackoffMs = 100;
        int requestTimeoutMs = 5000;
        Set<Integer> voters = Collections.singleton(localId);
        ElectionStore store = new MockElectionStore();
        Time time = new MockTime();
        ReplicatedLog log = new MockLog();
        NetworkChannel channel = new MockNetworkChannel();
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, store, logContext);
        return new RaftManager(channel, log, quorum, time, electionTimeoutMs, electionJitterMs,
                retryBackoffMs, requestTimeoutMs, logContext);
    }

    @Test
    public void testPutAndGet() throws Exception {
        RaftManager manager = setupSingleNodeRaftManager();
        manager.initialize();
        SimpleKeyValueStore<Integer, Integer> store = new SimpleKeyValueStore<>(manager,
                new Serdes.IntegerSerde(), new Serdes.IntegerSerde());

        CompletableFuture<OffsetAndEpoch> future = store.put(0, 1);
        manager.poll();
        store.sync();
        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(0L, 1), future.get());
        assertEquals(1, store.get(0).intValue());
    }

}