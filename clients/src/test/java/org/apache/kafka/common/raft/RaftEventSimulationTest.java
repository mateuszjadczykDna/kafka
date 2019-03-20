package org.apache.kafka.common.raft;

import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class RaftEventSimulationTest {
    private static final int ELECTION_TIMEOUT_MS = 1000;
    private static final int ELECTION_JITTER_MS = 0;
    private static final int RETRY_BACKOFF_MS = 50;
    private static final int REQUEST_TIMEOUT_MS = 500;

    @Test
    public void testInitialLeaderElectionQuorumSizeOne() {
        testInitialLeaderElection(new QuorumConfig(1, 0));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeTwo() {
        testInitialLeaderElection(new QuorumConfig(2, 0));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeThree() {
        testInitialLeaderElection(new QuorumConfig(3, 0));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeFour() {
        testInitialLeaderElection(new QuorumConfig(4, 0));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeFive() {
        testInitialLeaderElection(new QuorumConfig(5, 0));
    }


    private void testInitialLeaderElection(QuorumConfig config) {
        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            cluster.startAll();
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 1);
            scheduler.runUntil(cluster::hasConsistentLeader);
        }
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeOne() {
        testReplicationNoLeaderChange(new QuorumConfig(1, 0));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeTwo() {
        testReplicationNoLeaderChange(new QuorumConfig(2, 0));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeThree() {
        testReplicationNoLeaderChange(new QuorumConfig(3, 0));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeFour() {
        testReplicationNoLeaderChange(new QuorumConfig(4, 0));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeFive() {
        testReplicationNoLeaderChange(new QuorumConfig(5, 0));
    }

    private void testReplicationNoLeaderChange(QuorumConfig config) {
        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 0 as the leader
            cluster.initializeElection(Election.withElectedLeader(2, 0));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 0);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.highWatermarkReached(10));
        }
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeThree() {
        testElectionAfterLeaderFailure(new QuorumConfig(3, 0));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFour() {
        testElectionAfterLeaderFailure(new QuorumConfig(4, 0));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFive() {
        testElectionAfterLeaderFailure(new QuorumConfig(5, 0));
    }

    private void testElectionAfterLeaderFailure(QuorumConfig config) {
        // We need at least three voters to run this tests
        assumeTrue(config.numVoters > 2);

        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 1 as the leader
            cluster.initializeElection(Election.withElectedLeader(2, 0));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            // Seed the cluster with some data
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 1);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.highWatermarkReached(10));

            // Kill the leader and write some more data. We can verify the new leader has been elected
            // by verifying that the high watermark can still advance.
            cluster.kill(1);
            scheduler.runUntil(() -> cluster.highWatermarkReached(20));
        }
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeThree() {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(3, 0));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFour() {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(4, 0));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFive() {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(5, 0));
    }

    private void testElectionAfterLeaderNetworkPartition(QuorumConfig config) {
        // We need at least three voters to run this tests
        assumeTrue(config.numVoters > 2);

        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 1 as the leader
            cluster.initializeElection(Election.withElectedLeader(2, 1));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            // Seed the cluster with some data
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 2);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.highWatermarkReached(10));

            // The leader gets partitioned off. We can verify the new leader has been elected
            // by writing some data and ensuring that
            router.filter(1, new DropAllTraffic());
            scheduler.runUntil(() -> cluster.highWatermarkReached(20));
        }
    }

    private EventScheduler schedulerWithDefaultInvariants(Cluster cluster) {
        EventScheduler scheduler = new EventScheduler(cluster.random, cluster.time);
        scheduler.addInvariant(new MonotonicHighWatermark(cluster));
        scheduler.addInvariant(new MonotonicEpoch(cluster));
        scheduler.addInvariant(new ConsistentCommittedData(cluster));
        return scheduler;
    }

    private void schedulePolling(EventScheduler scheduler,
                                 Cluster cluster,
                                 int pollIntervalMs,
                                 int pollJitterMs) {
        int delayMs = 0;
        for (int nodeId : cluster.nodes()) {
            scheduler.schedule(() -> cluster.pollIfRunning(nodeId), delayMs, pollIntervalMs, pollJitterMs);
            delayMs++;
        }
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
        final Cluster cluster;

        private SequentialAppendAction(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void execute() {
            cluster.forRandomRunning(node -> node.counter.increment());
        }
    }

    @FunctionalInterface
    private interface Invariant {
        void verify();
    }

    private static class EventScheduler {
        final AtomicInteger eventIdGenerator = new AtomicInteger(0);
        final PriorityQueue<Event> queue = new PriorityQueue<>();
        final Random random;
        final Time time;
        final List<Invariant> invariants = new ArrayList<>();

        private EventScheduler(Random random, Time time) {
            this.random = random;
            this.time = time;
        }

        private void addInvariant(Invariant invariant) {
            invariants.add(invariant);
        }

        void schedule(Action action, int delayMs, int periodMs, int jitterMs) {
            long initialDeadlineMs = time.milliseconds() + delayMs;
            int eventId = eventIdGenerator.incrementAndGet();
            PeriodicEvent event = new PeriodicEvent(action, eventId, random, initialDeadlineMs, periodMs, jitterMs);
            queue.offer(event);
        }

        void runUntil(Supplier<Boolean> exitCondition) {
            while (!exitCondition.get()) {
                if (queue.isEmpty())
                    throw new IllegalStateException("Event queue exhausted before condition was satisfied");
                Event event = queue.poll();
                long delayMs = Math.max(event.deadlineMs - time.milliseconds(), 0);
                time.sleep(delayMs);
                event.execute(this);
                for (Invariant invariant : invariants)
                    invariant.verify();

            }
        }
    }

    private static class QuorumConfig {
        final int numVoters;
        final int numObservers;

        private QuorumConfig(int numVoters, int numObservers) {
            this.numVoters = numVoters;
            this.numObservers = numObservers;
        }
    }

    private static class PersistentState {
        final MockElectionStore store = new MockElectionStore();
        final MockLog log = new MockLog();
    }

    private static class Cluster {
        final Random random;
        final AtomicInteger requestIcCounter = new AtomicInteger();
        final Time time = new MockTime();
        final Set<Integer> voters = new HashSet<>();
        final Map<Integer, PersistentState> nodes = new HashMap<>();
        final Map<Integer, RaftNode> running = new HashMap<>();

        private Cluster(QuorumConfig config, int randomSeed) {
            this.random = new Random(randomSeed);

            for (int voterId = 0; voterId < config.numVoters; voterId++) {
                voters.add(voterId);
                nodes.put(voterId, new PersistentState());
            }

            for (int nonVoterId = 0; nonVoterId < config.numObservers; nonVoterId++) {
                nodes.put(100 + nonVoterId, new PersistentState());
            }
        }

        Set<Integer> nodes() {
            return nodes.keySet();
        }

        Set<Integer> observers() {
            Set<Integer> observers = new HashSet<>(nodes.keySet());
            observers.removeAll(voters);
            return observers;
        }

        Set<Integer> voters() {
            return nodes.keySet();
        }

        OptionalLong leaderHighWatermark() {
            Optional<RaftNode> leaderWithMaxEpoch = running.values().stream().filter(node -> node.quorum.isLeader())
                    .max((node1, node2) -> Integer.compare(node2.quorum.epoch(), node1.quorum.epoch()));
            if (leaderWithMaxEpoch.isPresent()) {
                return leaderWithMaxEpoch.get().manager.highWatermark();
            } else {
                return OptionalLong.empty();
            }
        }

        boolean highWatermarkReached(long offset) {
            return running.values().stream()
                    .anyMatch(node -> node.quorum.highWatermark().orElse(0) > offset);
        }

        boolean hasConsistentLeader() {
            Iterator<RaftNode> iter = running.values().iterator();
            if (!iter.hasNext())
                return false;

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

        void kill(int nodeId) {
            running.remove(nodeId);
        }

        void pollIfRunning(int nodeId) {
            ifRunning(nodeId, RaftNode::poll);
        }

        Optional<RaftNode> nodeIfRunning(int nodeId) {
            return Optional.ofNullable(running.get(nodeId));
        }

        Collection<RaftNode> running() {
            return running.values();
        }

        void initializeElection(Election election) {
            if (election.hasLeader() && !voters.contains(election.leaderId()))
                throw new IllegalArgumentException("Illegal election of observer " + election.leaderId());

            nodes.values().forEach(state -> {
                state.store.write(election);
                if (election.hasLeader()) {
                    Optional<EndOffset> endOffset = state.log.endOffsetForEpoch(election.epoch);
                    if (!endOffset.isPresent())
                        state.log.assignEpochStartOffset(election.epoch, state.log.endOffset());
                }
            });
        }

        void ifRunning(int nodeId, Consumer<RaftNode> action) {
            nodeIfRunning(nodeId).ifPresent(action);
        }

        void forRandomRunning(Consumer<RaftNode> action) {
            List<RaftNode> nodes = new ArrayList<>(running.values());
            if (!nodes.isEmpty()) {
                RaftNode randomNode = nodes.get(random.nextInt(nodes.size()));
                action.accept(randomNode);
            }
        }

        void forAllRunning(Consumer<RaftNode> action) {
            running.values().forEach(action);
        }

        void startAll() {
            if (!running.isEmpty())
                throw new IllegalStateException("Some nodes are already started");
            for (int voterId : nodes.keySet())
                start(voterId);
        }

        void start(int nodeId) {
            LogContext logContext = new LogContext("[Node " + nodeId + "] ");
            PersistentState persistentState = nodes.get(nodeId);
            MockNetworkChannel channel = new MockNetworkChannel(requestIcCounter);
            QuorumState quorum = new QuorumState(nodeId, voters(), persistentState.store, logContext);
            RaftManager raftManager = new RaftManager(channel, persistentState.log, quorum, time,
                    ELECTION_TIMEOUT_MS, ELECTION_JITTER_MS, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS, logContext);
            RaftNode node = new RaftNode(nodeId, raftManager, persistentState.log, channel,
                    persistentState.store, quorum, logContext);
            node.initialize();
            running.put(nodeId, node);
        }
    }

    private static class RaftNode {
        final int nodeId;
        final RaftManager manager;
        final MockLog log;
        final MockNetworkChannel channel;
        final MockElectionStore store;
        final QuorumState quorum;
        final LogContext logContext;
        DistributedCounter counter;

        private RaftNode(int nodeId,
                         RaftManager manager,
                         MockLog log,
                         MockNetworkChannel channel,
                         MockElectionStore store,
                         QuorumState quorum,
                         LogContext logContext) {
            this.nodeId = nodeId;
            this.manager = manager;
            this.log = log;
            this.channel = channel;
            this.store = store;
            this.quorum = quorum;
            this.logContext = logContext;
        }

        void initialize() {
            this.counter = new DistributedCounter(manager, logContext);
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

    private interface NetworkFilter {
        boolean acceptInbound(RaftMessage message);
        boolean acceptOutbound(RaftMessage message);
    }

    private static class PermitAllTraffic implements NetworkFilter {

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return true;
        }

        @Override
        public boolean acceptOutbound(RaftMessage message) {
            return true;
        }
    }

    private static class DropAllTraffic implements NetworkFilter {

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return false;
        }

        @Override
        public boolean acceptOutbound(RaftMessage message) {
            return false;
        }
    }

    private static class MonotonicEpoch implements Invariant {
        final Cluster cluster;
        final Map<Integer, Integer> nodeEpochs = new HashMap<>();

        private MonotonicEpoch(Cluster cluster) {
            this.cluster = cluster;
            for (Map.Entry<Integer, PersistentState> nodeStateEntry : cluster.nodes.entrySet()) {
                Integer nodeId = nodeStateEntry.getKey();
                PersistentState state = nodeStateEntry.getValue();
                nodeEpochs.put(nodeId, state.store.read().epoch);
            }
        }

        @Override
        public void verify() {
            for (Map.Entry<Integer, PersistentState> nodeStateEntry : cluster.nodes.entrySet()) {
                Integer nodeId = nodeStateEntry.getKey();
                PersistentState state = nodeStateEntry.getValue();
                Integer oldEpoch = nodeEpochs.get(nodeId);
                Integer newEpoch = state.store.read().epoch;
                if (oldEpoch > newEpoch) {
                    fail("Non-monotonic update of high watermark detected: " +
                            oldEpoch + " -> " + newEpoch);
                }
                cluster.ifRunning(nodeId, nodeState -> {
                    assertEquals(newEpoch.intValue(), nodeState.quorum.epoch());
                });
                nodeEpochs.put(nodeId, newEpoch);
            }
        }
    }

    private static class MonotonicHighWatermark implements Invariant {
        final Cluster cluster;
        long highWatermark = 0;

        private MonotonicHighWatermark(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            OptionalLong leaderHighWatermark = cluster.leaderHighWatermark();
            leaderHighWatermark.ifPresent(newHighWatermark -> {
                long oldHighWatermark = highWatermark;
                this.highWatermark = newHighWatermark;
                if (newHighWatermark < oldHighWatermark) {
                    fail("Non-monotonic update of high watermark detected: " +
                            oldHighWatermark + " -> " + newHighWatermark);
                }
            });
        }
    }

    private static class ConsistentCommittedData implements Invariant {
        final Cluster cluster;
        final List<Integer> committedLog = new ArrayList<>();

        private ConsistentCommittedData(Cluster cluster) {
            this.cluster = cluster;
        }

        private Integer parseSequenceNumber(MockLog.LogEntry entry) {
            ByteBuffer value = entry.record.value().duplicate();
            return (Integer) Type.INT32.read(value);
        }

        private void assertCommittedData(RaftManager manager, MockLog log) {
            manager.highWatermark().ifPresent(highWatermark -> {
                List<MockLog.LogEntry> entries = log.readEntries(0L, highWatermark);
                if (committedLog.size() < entries.size()) {
                    for (int i = committedLog.size(); i < entries.size(); i++) {
                        committedLog.add(parseSequenceNumber(entries.get(i)));
                    }
                }

                for (int i = 0; i < entries.size(); i++) {
                    Integer previousCommittedSequence = committedLog.get(i);
                    Integer newCommitteSequence = parseSequenceNumber(entries.get(i));
                    assertEquals("Committed sequence for a given offset should never change",
                            previousCommittedSequence, newCommitteSequence);
                }
            });
        }

        @Override
        public void verify() {
            cluster.forAllRunning(node -> assertCommittedData(node.manager, node.log));
        }
    }

    private static class MessageRouter {
        final Map<Integer, InflightRequest> inflight = new HashMap<>();
        final Map<Integer, NetworkFilter> filters = new HashMap<>();
        final Cluster cluster;

        private MessageRouter(Cluster cluster) {
            this.cluster = cluster;
            for (int nodeId : cluster.nodes.keySet())
                filters.put(nodeId, new PermitAllTraffic());
        }

        void deliver(int senderId, RaftRequest.Outbound outbound) {
            int requestId = outbound.requestId();
            int destinationId = outbound.destinationId();
            RaftRequest.Inbound inbound = new RaftRequest.Inbound(requestId, outbound.data(),
                    cluster.time.milliseconds());
            if (!filters.get(destinationId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(destinationId).ifPresent(node -> {
                MockNetworkChannel destChannel = node.channel;
                inflight.put(requestId, new InflightRequest(requestId, senderId, destinationId));
                destChannel.mockReceive(inbound);
            });
        }

        void deliver(int senderId, RaftResponse.Outbound outbound) {
            int requestId = outbound.requestId();
            RaftResponse.Inbound inbound = new RaftResponse.Inbound(requestId, outbound.data(), senderId);
            InflightRequest inflightRequest = inflight.remove(requestId);
            if (!filters.get(inflightRequest.sourceId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(inflightRequest.sourceId).ifPresent(node -> {
                node.channel.mockReceive(inbound);
            });
        }

        void deliver(int senderId, RaftMessage message) {
            if (!filters.get(senderId).acceptOutbound(message)) {
                return;
            } else if (message instanceof RaftRequest.Outbound) {
                deliver(senderId, (RaftRequest.Outbound) message);
            } else if (message instanceof RaftResponse.Outbound) {
                deliver(senderId, (RaftResponse.Outbound) message);
            } else {
                throw new AssertionError("Illegal message type sent by node " + message);
            }
        }

        void filter(int nodeId, NetworkFilter filter) {
            filters.put(nodeId, filter);
        }

        void deliverRandom() {
            cluster.forRandomRunning(this::deliverTo);
        }

        void deliverTo(RaftNode node) {
            node.channel.drainSendQueue().forEach(msg -> deliver(node.nodeId, msg));
        }

        void deliverAll() {
            for (RaftNode node : cluster.running()) {
                deliverTo(node);
            }
        }
    }

}
