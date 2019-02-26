package org.apache.kafka.common.raft;

import org.apache.kafka.common.message.AppendRecordsRequestData;
import org.apache.kafka.common.message.AppendRecordsResponseData;
import org.apache.kafka.common.message.BeginEpochRequestData;
import org.apache.kafka.common.message.EndEpochRequestData;
import org.apache.kafka.common.message.FetchEndOffsetRequestData;
import org.apache.kafka.common.message.FetchEndOffsetResponseData;
import org.apache.kafka.common.message.FetchRecordsRequestData;
import org.apache.kafka.common.message.FetchRecordsResponseData;
import org.apache.kafka.common.message.FindLeaderRequestData;
import org.apache.kafka.common.message.FindLeaderResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RaftManagerTest {
    private final int localId = 0;
    private final int electionTimeoutMs = 10000;
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 5000;
    private final int electionJitterMs = 0;
    private final MockTime time = new MockTime();
    private final MockElectionStore electionStore = new MockElectionStore();
    private final MockLog log = new MockLog();
    private final MockNetworkChannel channel = new MockNetworkChannel();

    private RaftManager buildManager(Set<Integer> voters) {
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, electionStore, logContext);
        return new RaftManager(channel, log, quorum, time, electionTimeoutMs, electionJitterMs,
                retryBackoffMs, requestTimeoutMs, logContext);
    }

    @Test
    public void testInitializeSingleMemberQuorum() {
        RaftManager manager = buildManager(Collections.singleton(localId));
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());
        manager.poll();
        assertEquals(0, channel.drainSendQueue().size());
    }

    @Test
    public void testInitializeAsCandidate() {
        int otherNodeId = 1;
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withVotedCandidate(1, localId), electionStore.read());

        manager.poll();

        int requestId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(requestId, voteResponse, otherNodeId));

        // Become leader after receiving the vote
        manager.poll();
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        // Send BeginEpoch to voters
        manager.poll();
        assertBeginEpochRequest(1, 0, 0L);
    }

    @Test
    public void testVoteRequestTimeout() {
        int otherNodeId = 1;
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withVotedCandidate(1, localId), electionStore.read());

        manager.poll();
        int requestId = assertSentVoteRequest(1, 0, 0L);

        time.sleep(requestTimeoutMs);
        manager.poll();
        int retryId = assertSentVoteRequest(1, 0, 0L);

        // Even though we have resent the request, we should still accept the response to
        // the first request if it arrives late.
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(requestId, voteResponse, otherNodeId));
        manager.poll();
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        // If the second request arrives later, it should have no effect
        VoteResponseData retryResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(retryId, retryResponse, otherNodeId));
        manager.poll();
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());
    }

    @Test
    public void testRetryElection() {
        int otherNodeId = 1;
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withVotedCandidate(1, localId), electionStore.read());

        manager.poll();

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int requestId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(false, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(requestId, voteResponse, otherNodeId));

        manager.poll();
        assertEquals(Election.withUnknownLeader(1), electionStore.read());

        // If no new election is held, we will become a candidate again after awaiting the backoff time
        time.sleep(retryBackoffMs);
        manager.poll();
        int retryId = assertSentVoteRequest(2, 0, 0L);
        VoteResponseData retryVoteResponse = voteResponse(true, Optional.empty(), 2);
        channel.mockReceive(new RaftResponse.Inbound(retryId, retryVoteResponse, otherNodeId));

        manager.poll();
        assertEquals(Election.withElectedLeader(2, localId), electionStore.read());
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() {
        int otherNodeId = 1;
        int epoch = 5;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());

        manager.poll();
        assertSentFetchRecordsRequest(epoch, 0L);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());

        manager.poll();
        assertSentFetchEndOffsetRequest(epoch, lastEpoch);
    }

    @Test
    public void testBecomeCandidateAfterElectionTimeout() {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());

        manager.poll();
        assertSentFetchEndOffsetRequest(epoch, lastEpoch);

        time.sleep(electionTimeoutMs);

        manager.poll();
        assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
    }

    @Test
    public void testInitializeObserverNoPreviousState() {
        int leaderId = 1;
        int epoch = 5;
        RaftManager manager = buildManager(Utils.mkSet(leaderId));

        manager.poll();
        int requestId = assertSentFindLeaderRequest();
        channel.mockReceive(new RaftResponse.Inbound(requestId, findLeaderResponse(leaderId, epoch), leaderId));

        manager.poll();
        assertEquals(Election.withElectedLeader(epoch, leaderId), electionStore.read());
    }

    @Test
    public void testObserverFindLeaderFailure() {
        int leaderId = 1;
        int epoch = 5;
        RaftManager manager = buildManager(Utils.mkSet(leaderId));

        manager.poll();
        int requestId = assertSentFindLeaderRequest();
        channel.mockReceive(new RaftResponse.Inbound(requestId, findLeaderFailure(Errors.UNKNOWN_SERVER_ERROR), leaderId));

        manager.poll();
        assertEquals(0, channel.drainSendQueue().size());

        time.sleep(retryBackoffMs);

        manager.poll();
        int retryId = assertSentFindLeaderRequest();
        channel.mockReceive(new RaftResponse.Inbound(retryId, findLeaderResponse(leaderId, epoch), leaderId));

        manager.poll();
        assertEquals(Election.withElectedLeader(epoch, leaderId), electionStore.read());
    }

    @Test
    public void testLeaderHandlesFindLeader() {
        RaftManager manager = buildManager(Collections.singleton(localId));
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        int observerId = 1;
        FindLeaderRequestData request = new FindLeaderRequestData().setReplicaId(observerId);
        channel.mockReceive(new RaftRequest.Inbound(channel.newRequestId(), request, time.milliseconds()));

        manager.poll();
        assertSentFindLeaderResponse(1, Optional.of(localId));
    }

    @Test
    public void testLeaderGracefulShutdown() {
        int otherNodeId = 1;
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));

        // Elect ourselves as the leader
        assertEquals(Election.withVotedCandidate(1, localId), electionStore.read());
        manager.poll();

        int voteRequestId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteRequestId, voteResponse, otherNodeId));
        manager.poll();
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        manager.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndEpoch
        assertTrue(manager.isRunning());

        // Send EndEpoch request to the other vote
        manager.poll();
        assertTrue(manager.isRunning());
        assertSentEndEpochRequest(1, localId);

        // Graceful shutdown completes when the epoch is bumped
        VoteRequestData newVoteRequest = voteRequest(2, otherNodeId, 0, 0L);
        channel.mockReceive(new RaftRequest.Inbound(channel.newRequestId(), newVoteRequest, time.milliseconds()));

        manager.poll();
        assertFalse(manager.isRunning());
    }

    @Test
    public void testLeaderGracefulShutdownTimeout() {
        int otherNodeId = 1;
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));

        // Elect ourselves as the leader
        assertEquals(Election.withVotedCandidate(1, localId), electionStore.read());
        manager.poll();

        int voteRequestId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteRequestId, voteResponse, otherNodeId));
        manager.poll();
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        manager.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndEpoch
        assertTrue(manager.isRunning());

        // Send EndEpoch request to the other vote
        manager.poll();
        assertTrue(manager.isRunning());
        assertSentEndEpochRequest(1, localId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        time.sleep(shutdownTimeoutMs);

        manager.poll();
        assertFalse(manager.isRunning());
    }

    @Test
    public void testFollowerGracefulShutdown() {
        int otherNodeId = 1;
        int epoch = 5;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());

        manager.poll();

        int shutdownTimeoutMs = 5000;
        manager.shutdown(shutdownTimeoutMs);
        assertTrue(manager.isRunning());
        manager.poll();
        assertFalse(manager.isRunning());
    }

    @Test
    public void testGracefulShutdownSingleMemberQuorum() {
        RaftManager manager = buildManager(Collections.singleton(localId));
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());
        manager.poll();
        assertEquals(0, channel.drainSendQueue().size());
        int shutdownTimeoutMs = 5000;
        manager.shutdown(shutdownTimeoutMs);
        assertTrue(manager.isRunning());
        manager.poll();
        assertFalse(manager.isRunning());
    }

    @Test
    public void testFollowerReplication() {
        int otherNodeId = 1;
        int epoch = 5;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());

        manager.poll();

        int requestId = assertSentFetchRecordsRequest(epoch, 0L);

        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
        3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchRecordsResponseData response = fetchRecordsResponse(epoch, otherNodeId, records, 0L);
        channel.mockReceive(new RaftResponse.Inbound(requestId, response, otherNodeId));

        manager.poll();
        assertEquals(2L, log.endOffset());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() {
        RaftManager manager = buildManager(Collections.singleton(localId));
        assertEquals(Election.withElectedLeader(1, localId), electionStore.read());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);

        // First append the data
        AppendRecordsRequestData appendRequest = appendRecordsRequest(1, records);
        int appendRequestId = channel.newRequestId();
        channel.mockReceive(new RaftRequest.Inbound(appendRequestId, appendRequest, time.milliseconds()));

        manager.poll();

        // The high watermark advances immediately since there is only a single node in the quorum
        assertEquals(appendRequestId, assertAppendRecordsResponse(1, localId, 0L, 3L));

        // Now try reading it
        int otherNodeId = 1;
        FetchRecordsRequestData fetchRequest = fetchRecordsRequest(1, otherNodeId, 0L);
        channel.mockReceive(new RaftRequest.Inbound(channel.newRequestId(), fetchRequest, time.milliseconds()));

        manager.poll();

        MemoryRecords fetchedRecords = assertFetchRecordsResponse(1, localId);
        List<MutableRecordBatch> batches = Utils.toList(fetchedRecords.batchIterator());
        assertEquals(1, batches.size());

        MutableRecordBatch batch = batches.get(0);
        assertEquals(1, batch.partitionLeaderEpoch());
        List<Record> readRecords = Utils.toList(batch.iterator());
        assertEquals(3, readRecords.size());
        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i].value(), readRecords.get(i).value());
        }
    }

    @Test
    public void testFollowerLogReconciliation() {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        electionStore.write(Election.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Arrays.asList(
                new SimpleRecord("foo".getBytes()),
                new SimpleRecord("bar".getBytes())), lastEpoch);

        RaftManager manager = buildManager(Utils.mkSet(localId, otherNodeId));
        assertEquals(Election.withElectedLeader(epoch, otherNodeId), electionStore.read());
        assertEquals(2L, log.endOffset());

        manager.poll();
        int requestId = assertSentFetchEndOffsetRequest(epoch, lastEpoch);

        FetchEndOffsetResponseData response = fetchEndOffsetResponse(epoch, otherNodeId, 1L, lastEpoch);
        channel.mockReceive(new RaftResponse.Inbound(requestId, response, otherNodeId));

        // Poll again to complete truncation
        manager.poll();
        assertEquals(1L, log.endOffset());

        // Now we should be fetching
        manager.poll();
        assertSentFetchRecordsRequest(epoch, 1L);
    }

    private int assertSentFindLeaderResponse(int epoch, Optional<Integer> leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindLeaderResponseData);
        FindLeaderResponseData response = (FindLeaderResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1).intValue(), response.leaderId());
        return raftMessage.requestId();
    }

    private int assertAppendRecordsResponse(int epoch, int leaderId, long baseOffset, long highWatermark) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof AppendRecordsResponseData);
        AppendRecordsResponseData response = (AppendRecordsResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId, response.leaderId());
        assertEquals(baseOffset, response.baseOffset());
        assertEquals(highWatermark, response.highWatermark());
        return raftMessage.requestId();
    }

    private MemoryRecords assertFetchRecordsResponse(int epoch, int leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FetchRecordsResponseData);
        FetchRecordsResponseData response = (FetchRecordsResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId, response.leaderId());
        return MemoryRecords.readableRecords(ByteBuffer.wrap(response.records()));
    }

    private int assertSentEndEpochRequest(int epoch, int leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndEpochRequestData);
        EndEpochRequestData request = (EndEpochRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(leaderId, request.leaderId());
        assertEquals(localId, request.replicaId());
        return raftMessage.requestId();
    }

    private int assertSentFindLeaderRequest() {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindLeaderRequestData);
        FindLeaderRequestData request = (FindLeaderRequestData) raftMessage.data();
        assertEquals(localId, request.replicaId());
        return raftMessage.requestId();
    }

    private int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteRequestData);
        VoteRequestData request = (VoteRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(localId, request.candidateId());
        assertEquals(lastEpoch, request.lastEpoch());
        assertEquals(lastEpochOffset, request.lastEpochEndOffset());
        return raftMessage.requestId();
    }

    private int assertBeginEpochRequest(int epoch, int prevEpoch, long prevEpochEndOffset) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginEpochRequestData);
        BeginEpochRequestData request = (BeginEpochRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(localId, request.leaderId());
        assertEquals(prevEpoch, request.previousEpoch());
        assertEquals(prevEpochEndOffset, request.previousEpochEndOffset());
        return raftMessage.requestId();
    }

    private int assertSentFetchEndOffsetRequest(int epoch, int lastEpoch) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FetchEndOffsetRequestData);
        FetchEndOffsetRequestData request = (FetchEndOffsetRequestData) raftMessage.data();
        assertEquals(lastEpoch, request.lastEpoch());
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(localId, request.replicaId());
        return raftMessage.requestId();
    }

    private int assertSentFetchRecordsRequest(int epoch, long fetchOffset) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FetchRecordsRequestData);
        FetchRecordsRequestData request = (FetchRecordsRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(fetchOffset, request.fetchOffset());
        assertEquals(localId, request.replicaId());
        return raftMessage.requestId();
    }

    private FetchRecordsResponseData fetchRecordsResponse(int epoch, int leaderId, Records records, long highWatermark) {
        return new FetchRecordsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(highWatermark)
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId)
                .setRecords(RaftUtil.serializeRecords(records));
    }

    private FetchEndOffsetResponseData fetchEndOffsetResponse(int epoch, int leaderId, long endOffset, int endOffsetEpoch) {
        return new FetchEndOffsetResponseData()
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId)
                .setEndOffset(endOffset)
                .setEndOffsetEpoch(endOffsetEpoch);
    }

    private VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return new VoteResponseData()
                .setVoteGranted(voteGranted)
                .setLeaderId(leaderId.orElse(-1))
                .setLeaderEpoch(epoch)
                .setErrorCode(Errors.NONE.code());
    }

    private VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return new VoteRequestData()
                .setLeaderEpoch(epoch)
                .setCandidateId(candidateId)
                .setLastEpoch(lastEpoch)
                .setLastEpochEndOffset(lastEpochOffset);
    }

    private FindLeaderResponseData findLeaderResponse(int leaderId, int epoch) {
        return new FindLeaderResponseData()
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);
    }

    private FindLeaderResponseData findLeaderFailure(Errors error) {
        return new FindLeaderResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(-1)
                .setLeaderId(-1);
    }

    private AppendRecordsRequestData appendRecordsRequest(int epoch, Records records) {
        return new AppendRecordsRequestData()
                .setLeaderEpoch(epoch)
                .setRecords(RaftUtil.serializeRecords(records));
    }

    private FetchRecordsRequestData fetchRecordsRequest(int epoch, int replicaId, long fetchOffset) {
        return new FetchRecordsRequestData()
                .setLeaderEpoch(epoch)
                .setFetchOffset(fetchOffset)
                .setReplicaId(replicaId);
    }

}
