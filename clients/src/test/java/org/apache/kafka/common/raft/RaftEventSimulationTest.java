package org.apache.kafka.common.raft;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RaftEventSimulationTest {

    private final int electionTimeoutMs = 1000;
    private final int electionJitterMs = 0;
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 500;

    @Test
    public void testElection() {
        Set<Integer> voters = Utils.mkSet(1, 2, 3);
        Time time = new MockTime();

        Map<Integer, RaftNode> nodes = buildQuorum(time, voters);
        MessageRouter router = new MessageRouter(time, nodes);
        nodes.values().forEach(RaftNode::initialize);

        Random random = new Random(1);
        EventScheduler scheduler = new EventScheduler(time, random);
        scheduler.schedule(() -> nodes.get(1).poll(), 0, 5, 5);
        scheduler.schedule(() -> nodes.get(2).poll(), 1, 5, 5);
        scheduler.schedule(() -> nodes.get(3).poll(), 3, 5, 5);
        scheduler.schedule(router::deliver, 0, 2, 0);
        scheduler.runUntil(() -> leaderAndEpochAgreement(nodes.values()));
    }

    @Test
    public void testReplicationNoLeaderChange() {
        Set<Integer> voters = Utils.mkSet(1, 2, 3);
        Time time = new MockTime();

        Map<Integer, RaftNode> nodes = buildQuorum(time, voters);
        MessageRouter router = new MessageRouter(time, nodes);
        // Force 1 to become the leader
        for (RaftNode node : nodes.values()) {
            node.store.write(Election.withElectedLeader(2, 1));
            node.initialize();
        }

        Random random = new Random(1);
        EventScheduler scheduler = new EventScheduler(time, random);
        scheduler.schedule(() -> nodes.get(1).poll(), 0, 5, 5);
        scheduler.schedule(() -> nodes.get(2).poll(), 1, 5, 5);
        scheduler.schedule(() -> nodes.get(3).poll(), 3, 5, 5);
        scheduler.schedule(router::deliver, 0, 2, 0);
        scheduler.schedule(new SequentialAppendAction(new ArrayList<>(nodes.values()), random),
                0, 2, 3);
        scheduler.runUntil(() -> highWatermarkReached(25, nodes.values()));
    }

    private boolean highWatermarkReached(long offset, Collection<RaftNode> nodes) {
        return nodes.stream().allMatch(node -> node.quorum.highWatermark().orElse(0) > offset);
    }

    private boolean leaderAndEpochAgreement(Collection<RaftNode> nodes) {
        Iterator<RaftNode> iter = nodes.iterator();
        RaftNode first = iter.next();
        Election election = first.store.read();
        if (!election.hasLeader())
            return false;

        while (iter.hasNext()) {
            RaftNode next = iter.next();
            if (!election.equals(next.store.read()))
                return false;
        }

        return true;
    }

    @FunctionalInterface
    private interface Action {
        void execute();
    }

    private static abstract class Event implements Comparable<Event> {
        final int eventId;
        final long deadlineMs;
        final Action action;

        protected Event(Action action, int eventId, long deadlineMs) {
            this.action = action;
            this.eventId = eventId;
            this.deadlineMs = deadlineMs;
        }

        void execute(EventScheduler scheduler) {
            action.execute();
        }

        public int compareTo(Event other) {
            int compare = Long.compare(deadlineMs, other.deadlineMs);
            if (compare != 0)
                return compare;
            return Integer.compare(eventId, other.eventId);
        }
    }

    private static class PeriodicEvent extends Event {
        final Random random;
        final int periodMs;
        final int jitterMs;

        protected PeriodicEvent(Action action,
                                int eventId,
                                Random random,
                                long deadlineMs,
                                int periodMs,
                                int jitterMs) {
            super(action, eventId, deadlineMs);
            this.random = random;
            this.periodMs = periodMs;
            this.jitterMs = jitterMs;
        }

        @Override
        void execute(EventScheduler scheduler) {
            super.execute(scheduler);
            int nextExecDelayMs = periodMs + (jitterMs == 0 ? 0 : random.nextInt(jitterMs));
            scheduler.schedule(action, nextExecDelayMs, periodMs, jitterMs);
        }
    }

    private static class SequentialAppendAction implements Action {
        final Random random;
        final List<RaftNode> nodes;
        final AtomicInteger sequence = new AtomicInteger(0);

        private SequentialAppendAction(List<RaftNode> nodes, Random random) {
            this.nodes = nodes;
            this.random = random;
        }

        @Override
        public void execute() {
            String value = sequence.getAndIncrement() + "";
            SimpleRecord record = new SimpleRecord(value.getBytes());
            MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE, record);

            // Choose random destination and append
            RaftNode node = nodes.get(random.nextInt(nodes.size()));
            node.manager.append(records);
        }
    }

    private static class EventScheduler {
        final Time time;
        final AtomicInteger eventIdGenerator = new AtomicInteger(0);
        final PriorityQueue<Event> queue = new PriorityQueue<>();
        final Random random;

        private EventScheduler(Time time, Random random) {
            this.time = time;
            this.random = random;
        }

        void schedule(Action action, int delayMs, int periodMs, int jitterMs) {
            long initialDeadlineMs = time.milliseconds() + delayMs;
            int eventId = eventIdGenerator.incrementAndGet();
            PeriodicEvent event = new PeriodicEvent(action, eventId, random,
                    initialDeadlineMs, periodMs, jitterMs);
            queue.offer(event);
        }

        void runUntil(Supplier<Boolean> condition) {
            while (!condition.get()) {
                if (queue.isEmpty())
                    throw new IllegalStateException("Event queue exhausted before condition was satisfied");
                Event event = queue.poll();
                long delayMs = Math.max(event.deadlineMs - time.milliseconds(), 0);
                time.sleep(delayMs);
                event.execute(this);
            }
        }
    }

    private Map<Integer, RaftNode> buildQuorum(Time time, Set<Integer> voters) {
        Map<Integer, RaftNode> nodes = new HashMap<>(voters.size());
        AtomicInteger requestIdCounter = new AtomicInteger(0);
        for (Integer voterId : voters) {
            nodes.put(voterId, buildNode(voterId, voters, time, requestIdCounter));
        }
        return nodes;
    }

    private RaftNode buildNode(int nodeId,
                               Set<Integer> voters,
                               Time time,
                               AtomicInteger requestCounter) {
        MockLog log = new MockLog();
        MockNetworkChannel channel = new MockNetworkChannel(requestCounter);
        LogContext logContext = new LogContext();
        MockElectionStore store = new MockElectionStore();
        QuorumState quorum = new QuorumState(nodeId, voters, store, logContext);
        RaftManager raftManager = new RaftManager(channel, log, quorum, time, electionTimeoutMs,
                electionJitterMs, retryBackoffMs, requestTimeoutMs, logContext);
        return new RaftNode(nodeId, raftManager, log, channel, store, quorum);
    }

    private static class RaftNode {
        final int nodeId;
        final RaftManager manager;
        final MockLog log;
        final MockNetworkChannel channel;
        final MockElectionStore store;
        final QuorumState quorum;

        private RaftNode(int nodeId,
                         RaftManager manager,
                         MockLog log,
                         MockNetworkChannel channel,
                         MockElectionStore store,
                         QuorumState quorum) {
            this.nodeId = nodeId;
            this.manager = manager;
            this.log = log;
            this.channel = channel;
            this.store = store;
            this.quorum = quorum;
        }

        void initialize() {
            manager.initialize();
        }

        void poll() {
            manager.poll();
        }
    }

    private static class InflightRequest {
        final int requestId;
        final int sourceId;
        final int destinationId;

        private InflightRequest(int requestId, int sourceId, int destinationId) {
            this.requestId = requestId;
            this.sourceId = sourceId;
            this.destinationId = destinationId;
        }
    }

    private static class MessageRouter {
        private Time time;
        private Map<Integer, RaftNode> nodes;
        private Map<Integer, InflightRequest> inflight = new HashMap<>();

        private MessageRouter(Time time, Map<Integer, RaftNode> nodes) {
            this.time = time;
            this.nodes = nodes;
        }

        void deliver(int senderId, RaftRequest.Outbound outbound) {
            int requestId = outbound.requestId();
            int destinationId = outbound.destinationId();
            MockNetworkChannel dest = nodes.get(destinationId).channel;
            RaftRequest.Inbound inbound = new RaftRequest.Inbound(requestId, outbound.data(), time.milliseconds());
            inflight.put(requestId, new InflightRequest(requestId, senderId, destinationId));
            dest.mockReceive(inbound);
        }

        void deliver(int senderId, RaftResponse.Outbound outbound) {
            int requestId = outbound.requestId();
            RaftResponse.Inbound inbound = new RaftResponse.Inbound(requestId, outbound.data(), senderId);
            InflightRequest inflightRequest = inflight.remove(requestId);
            RaftNode src = nodes.get(inflightRequest.sourceId);
            src.channel.mockReceive(inbound);
        }

        void deliver(int sourceId, RaftMessage message) {
            if (message instanceof RaftRequest.Outbound) {
                deliver(sourceId, (RaftRequest.Outbound) message);
            } else if (message instanceof RaftResponse.Outbound) {
                deliver(sourceId, (RaftResponse.Outbound) message);
            } else {
                throw new AssertionError("Illegal message type sent by node " + message);
            }
        }

        void deliver() {
            for (RaftNode node : nodes.values()) {
                node.channel.drainSendQueue().forEach(msg -> deliver(node.nodeId, msg));
            }
        }

    }

}
