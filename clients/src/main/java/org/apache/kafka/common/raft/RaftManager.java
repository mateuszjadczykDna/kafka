package org.apache.kafka.common.raft;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.message.AppendRecordsRequestData;
import org.apache.kafka.common.message.AppendRecordsResponseData;
import org.apache.kafka.common.message.BeginEpochRequestData;
import org.apache.kafka.common.message.BeginEpochResponseData;
import org.apache.kafka.common.message.EndEpochRequestData;
import org.apache.kafka.common.message.EndEpochResponseData;
import org.apache.kafka.common.message.FetchEndOffsetRequestData;
import org.apache.kafka.common.message.FetchEndOffsetResponseData;
import org.apache.kafka.common.message.FetchRecordsRequestData;
import org.apache.kafka.common.message.FetchRecordsResponseData;
import org.apache.kafka.common.message.FindLeaderRequestData;
import org.apache.kafka.common.message.FindLeaderResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

// TODO:
// - Handle out of range fetches
// - Figure out locking (probably read-write lock like normal partitions)
// - Uses of Timer.currentTimeMs are probably wrong
// - Deletion of old log data
// - How shall we track pending appends?
// - Let's have FetchRecords take fetch offset and epoch: a possible optimization
//   on leader failover is to skip the truncation check and just begin fetching
// - Should append requests use a separate connection?

/**
 * This class implements a Kafkaesque version of the Raft protocol. Leader election
 * is more or less pure Raft, but replication is driven by replica fetching and we use Kafka's
 * log reconciliation protocol to truncate the log to a common point following each leader
 * election.
 *
 * Like Zookeeper, this protocol distinguishes between voters and observers. Voters are
 * the only ones who are eligible to handle protocol requests and they are the only ones
 * who take part in elections. The protocol does not yet support dynamic quorum changes.
 *
 * There are seven request types in this protocol:
 *
 * 1) {@link VoteRequestData}: Sent by valid voters when their election timeout expires and they
 *    become a candidate. This request includes the last offset in the log which electors use
 *    to tell whether or not to grant the vote.
 *
 * 2) {@link BeginEpochRequestData}: Sent by the leader of an epoch only to valid voters to
 *    assert its leadership of the new epoch. This request will be retried indefinitely for
 *    each voter until it acknowledges the request or a new election occurs.
 *
 *    This is not needed in usual Raft because the leader can use an empty data push
 *    to achieve the same purpose. The Kafka Raft implementation, however, is driven by
 *    fetch requests from followers, so there must be a way to find the new leader after
 *    an election has completed.
 *
 *    We might consider replacing this API and let followers use FindLeader even if they
 *    are voters.
 *
 * 3) {@link EndEpochRequestData}: Sent by the leader of an epoch to valid voters in order to
 *    gracefully resign from the current epoch. This causes remaining voters to immediately
 *    begin a new election.
 *
 * 4) {@link FetchRecordsRequestData}: This is basically the same as the usual Fetch API in
 *    Kafka, however the protocol implements it as a separate request type because there
 *    is additional metadata which we need to piggyback on responses.
 *
 * 5) {@link FetchEndOffsetRequestData}: Analogous to the current OffsetsForLeaderEpoch API, but
 *    with the extra metadata piggy-backed onto it, like with FetchRecords.
 *
 * 6) {@link FindLeaderRequestData}: Sent by observers in order to find the leader. The leader
 *    is responsible for pushing BeginEpoch requests to other votes, but it is the responsibility
 *    of observers to find the current leader themselves. We could probably use one of the Fetch
 *    APIs for the same purpose, but we separated it initially for clarity.
 *
 * 7) {@link AppendRecordsRequestData}: Sent by non-leaders in order to append new records
 *    to the log.
 */
public class RaftManager {
    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final Logger logger;
    private final Time time;
    private final Timer electionTimer;
    private final int electionTimeoutMs;
    private final int electionJitterMs;
    private final int retryBackoffMs;
    private final int requestTimeoutMs;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final QuorumState quorum;
    private final Random random = new Random();
    private final Map<Integer, ConnectionState> voterConnections;

    private ConnectionState findLeaderConnection;
    private boolean awaitingTruncation;
    private BlockingQueue<PendingAppendRequest> unsentAppends;
    private Map<Integer, PendingAppendRequest> sentAppends;

    public RaftManager(NetworkChannel channel,
                       ReplicatedLog log,
                       QuorumState quorum,
                       Time time,
                       int electionTimeoutMs,
                       int electionJitterMs,
                       int retryBackoffMs,
                       int requestTimeoutMs,
                       LogContext logContext) {
        this.channel = channel;
        this.log = log;
        this.quorum = quorum;
        this.time = time;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.retryBackoffMs = retryBackoffMs;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionJitterMs = electionJitterMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.logger = logContext.logger(RaftManager.class);
        this.voterConnections = new HashMap<>();
        this.sentAppends = new HashMap<>();
        this.unsentAppends = new ArrayBlockingQueue<>(10);
    }

    private boolean isLogNonEmpty() {
        return log.latestEpoch() != 0;
    }

    /**
     * On startup:
     * - If there is no election state, become a candidate
     * - If we were the last leader, become a candidate
     * - If we were a follower, become a follower and check for truncation
     */
    public void initialize() {
        quorum.initialize();

        for (Integer voterId : quorum.remoteVoters()) {
            voterConnections.put(voterId, new ConnectionState(retryBackoffMs, requestTimeoutMs));
        }
        findLeaderConnection = new ConnectionState(retryBackoffMs, requestTimeoutMs);

        // If the quorum consists of a single node, we can become leader immediately
        if (quorum.isCandidate()) {
            maybeBecomeLeader(quorum.candidateStateOrThrow());
        } else if (quorum.isFollower()) {
            this.awaitingTruncation = isLogNonEmpty();
        }
    }

    private EndOffset endOffset() {
        return new EndOffset(log.endOffset(), log.latestEpoch());
    }

    private void resetConnections() {
        for (ConnectionState connectionState : voterConnections.values())
            connectionState.reset();
        findLeaderConnection.reset();
    }

    private void maybeBecomeLeader(CandidateState state) {
        if (state.isVoteGranted()) {
            long endOffset = log.endOffset();
            LeaderState leaderState = quorum.becomeLeader();
            leaderState.updateLocalEndOffset(endOffset);
            log.assignEpochStartOffset(quorum.epoch(), endOffset);
            electionTimer.reset(Long.MAX_VALUE);
            resetConnections();
        }
    }

    private void becomeCandidate() {
        electionTimer.reset(electionTimeoutMs);
        CandidateState state = quorum.becomeCandidate();
        maybeBecomeLeader(state);
        resetConnections();
    }

    private void becomeUnattachedFollower(int epoch) {
        if (quorum.becomeUnattachedFollower(epoch)) {
            resetConnections();
        }
    }

    private void becomeVotedFollower(int candidateId, int epoch) {
        if (quorum.becomeVotedFollower(epoch, candidateId)) {
            electionTimer.reset(electionTimeoutMs);
            resetConnections();
        }
    }

    private void becomeFollower(int leaderId, int epoch) {
        if (quorum.becomeFollower(epoch, leaderId)) {
            electionTimer.reset(electionTimeoutMs);
            awaitingTruncation = isLogNonEmpty();
            resetConnections();
        }
    }

    private void maybeBeginFetching(EndOffset prevEpochEndOffset) {
        if (log.truncateToEndOffset(prevEpochEndOffset)) {
            awaitingTruncation = false;
        }
    }

    private VoteResponseData buildVoteResponse(Errors error, boolean voteGranted) {
        return new VoteResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setVoteGranted(voteGranted);
    }

    private VoteResponseData handleVoteRequest(VoteRequestData request) {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.candidateId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildVoteResponse(Errors.forException(errorOpt.get()), false);
        }

        return quorum.visit(new QuorumState.Visitor<VoteResponseData>() {
            @Override
            public VoteResponseData ifFollower(FollowerState state) {
                int candidateId = request.candidateId();
                final boolean voteGranted;
                if (state.hasLeader()) {
                    voteGranted = false;
                } else if (state.hasVoted()) {
                    voteGranted = state.isVotedCandidate(candidateId);
                } else {
                    EndOffset lastEpochEndOffset = new EndOffset(request.lastEpochEndOffset(), request.lastEpoch());
                    voteGranted = lastEpochEndOffset.compareTo(endOffset()) >= 0;
                }

                if (voteGranted)
                    becomeVotedFollower(candidateId, request.leaderEpoch());
                return buildVoteResponse(Errors.NONE, voteGranted);
            }

            @Override
            public VoteResponseData ifLeader(LeaderState state) {
                logger.debug("Ignoring vote request {} since we are the leader of epoch {}",
                        request, quorum.epoch());
                return buildVoteResponse(Errors.NONE, false);
            }

            @Override
            public VoteResponseData ifCandidate(CandidateState state) {
                logger.debug("Ignoring vote request {} since we are a candidate of epoch {}",
                        request, quorum.epoch());
                return buildVoteResponse(Errors.NONE, false);
            }
        });
    }

    private void handleVoteResponse(int remoteNodeId, VoteResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId))
            return;

        quorum.visit(new QuorumState.VoidVisitor() {
            @Override
            public void ifFollower(FollowerState state) {
                logger.debug("Ignoring vote response {} since we are now a follower for epoch {}",
                        response, quorum.epoch());
            }

            @Override
            public void ifLeader(LeaderState state) {
                logger.debug("Ignoring vote response {} since we already became leader for epoch {}",
                        response, quorum.epoch());
            }

            @Override
            public void ifCandidate(CandidateState state) {
                if (response.voteGranted()) {
                    state.voteGrantedBy(remoteNodeId);
                    maybeBecomeLeader(state);
                } else {
                    state.voteRejectedBy(remoteNodeId);
                    if (state.isVoteRejected()) {
                        logger.info("A majority of voters rejected our candidacy, so we will become a follower");
                        becomeUnattachedFollower(quorum.epoch());

                        // We will retry our candidacy after a short backoff if no leader is elected
                        electionTimer.reset(retryBackoffMs + randomElectionJitterMs());
                    }
                }
            }
        });
    }

    private int randomElectionJitterMs() {
        if (electionJitterMs == 0)
            return 0;
        return random.nextInt(electionJitterMs);
    }

    private BeginEpochResponseData buildBeginEpochResponse(Errors error) {
        return new BeginEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private BeginEpochResponseData handleBeginEpochRequest(BeginEpochRequestData request) {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildBeginEpochResponse(Errors.forException(errorOpt.get()));
        }

        int requestLeaderId = request.leaderId();
        int requestEpoch = request.leaderEpoch();
        becomeFollower(requestLeaderId, requestEpoch);
        EndOffset prevEpochEndOffset = new EndOffset(request.previousEpochEndOffset(), request.previousEpoch());
        maybeBeginFetching(prevEpochEndOffset);
        return buildBeginEpochResponse(Errors.NONE);
    }

    private void handleBeginEpochResponse(int remoteNodeId, BeginEpochResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId))
            return;

        LeaderState state = quorum.leaderStateOrThrow();
        state.addEndorsementFrom(remoteNodeId);
    }

    private EndEpochResponseData buildEndEpochResponse(Errors error) {
        return new EndEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private EndEpochResponseData handleEndEpochRequest(EndEpochRequestData request) {
        Optional<Exception> errorOpt = handleInvalidVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEndEpochResponse(Errors.forException(errorOpt.get()));
        }

        // Regardless of our current state, we will become a candidate.
        // Do not become a candidate immediately though.
        electionTimer.reset(randomElectionJitterMs());
        return buildEndEpochResponse(Errors.NONE);
    }

    private void handleEndEpochResponse(EndEpochResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId);
    }

    private FetchRecordsResponseData buildFetchRecordsResponse(Errors error,
                                                               Records records,
                                                               OptionalLong highWatermark) {
        return new FetchRecordsResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setRecords(RaftUtil.serializeRecords(records))
                .setHighWatermark(highWatermark.orElse(-1L));
    }

    private FetchRecordsResponseData handleFetchRecordsRequest(FetchRecordsRequestData request) {
        Optional<Exception> errorOpt = handleInvalidLeaderOnlyRequest(request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildFetchRecordsResponse(Errors.forException(errorOpt.get()), MemoryRecords.EMPTY,
                    OptionalLong.empty());
        }

        LeaderState state = quorum.leaderStateOrThrow();
        long fetchOffset = request.fetchOffset();
        int replicaId = request.replicaId();
        OptionalLong highWatermark = state.highWatermark();

        if (quorum.isVoter(replicaId)) {
            // Voters can read to the end of the log
            state.updateEndOffset(replicaId, fetchOffset);
            Records records = log.read(fetchOffset, log.endOffset());
            return buildFetchRecordsResponse(Errors.NONE, records, highWatermark);
        } else {
            Records records = highWatermark.isPresent() ?
                    log.read(fetchOffset, highWatermark.getAsLong()) :
                    MemoryRecords.EMPTY;
            return buildFetchRecordsResponse(Errors.NONE, records, highWatermark);
        }
    }

    private OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    private void handleFetchRecordsResponse(FetchRecordsResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId))
            return;

        FollowerState state = quorum.followerStateOrThrow();
        if (awaitingTruncation) {
            logger.info("Ignoring invalid fetch response {} while awaiting truncation for epoch {}",
                    response, quorum.epoch());
        } else {
            ByteBuffer recordsBuffer = ByteBuffer.wrap(response.records());
            log.appendAsFollower(MemoryRecords.readableRecords(recordsBuffer));

            OptionalLong highWatermark = response.highWatermark() < 0 ?
                    OptionalLong.empty() : OptionalLong.of(response.highWatermark());
            state.updateHighWatermark(highWatermark);
            electionTimer.reset(electionTimeoutMs);
        }
    }

    private void handleAppendRecordsResponse(int requestId, AppendRecordsResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId))
            return;

        FollowerState state = quorum.followerStateOrThrow();
        state.updateHighWatermark(OptionalLong.of(response.highWatermark()));
        PendingAppendRequest pendingAppend = sentAppends.get(requestId);
        if (pendingAppend != null) {
            pendingAppend.complete(new OffsetAndEpoch(response.baseOffset(), response.leaderEpoch()));
        }
    }

    private AppendRecordsResponseData buildAppendRecordsResponse(Errors error,
                                                                 OptionalLong baseOffset,
                                                                 OptionalLong highWatermark) {
        return new AppendRecordsResponseData()
                .setErrorCode(error.code())
                .setBaseOffset(baseOffset.orElse(-1))
                .setHighWatermark(highWatermark.orElse(-1))
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private AppendRecordsResponseData handleAppendRecordsRequest(AppendRecordsRequestData request) {
        Optional<Exception> errorOpt = handleInvalidLeaderOnlyRequest(request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildAppendRecordsResponse(Errors.forException(errorOpt.get()),
                    OptionalLong.empty(), OptionalLong.empty());
        }

        LeaderState state = quorum.leaderStateOrThrow();
        ByteBuffer buffer = ByteBuffer.wrap(request.records());
        Records records = MemoryRecords.readableRecords(buffer);
        Long baseOffset = log.appendAsLeader(records, quorum.epoch());
        state.updateLocalEndOffset(log.endOffset());
        return buildAppendRecordsResponse(Errors.NONE, OptionalLong.of(baseOffset), state.highWatermark());
    }

    private FetchEndOffsetResponseData buildFetchEndOffsetResponse(Errors error, EndOffset endOffset) {
        return new FetchEndOffsetResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setEndOffset(endOffset.offset)
                .setEndOffsetEpoch(endOffset.epoch);
    }

    private FetchEndOffsetResponseData handleFetchEndOffsetRequest(FetchEndOffsetRequestData request) {
        Optional<Exception> errorOpt = handleInvalidLeaderOnlyRequest(request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildFetchEndOffsetResponse(Errors.forException(errorOpt.get()), new EndOffset(-1, -1));
        }

        LeaderState state = quorum.leaderStateOrThrow();
        state.addEndorsementFrom(request.replicaId());
        EndOffset endOffset = log.endOffsetForEpoch(request.lastEpoch())
                .orElse(new EndOffset(-1L, -1));
        return buildFetchEndOffsetResponse(Errors.NONE, endOffset);
    }

    private void handleFetchEndOffsetResponse(FetchEndOffsetResponseData response) {
        OptionalInt leaderId = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderId))
            return;

        FollowerState state = quorum.followerStateOrThrow();
        if (!awaitingTruncation) {
            logger.warn("Received unexpected FetchEndOffset response {} while fetching", response);
        } else {
            if (response.endOffset() < 0 || response.endOffsetEpoch() < 0) {
                logger.warn("Leader returned an unknown offset to our EndOffset request");
            } else {
                EndOffset endOffset = new EndOffset(response.endOffset(), response.endOffsetEpoch());
                maybeBeginFetching(endOffset);
                electionTimer.reset(electionTimeoutMs);
            }
        }
    }

    private FindLeaderResponseData handleFindLeaderRequest() {
        // Only voters are allowed to handle FindLeader requests
        Errors error = Errors.NONE;
        if (quorum.isObserver()) {
            error = Errors.INVALID_REQUEST;
        } else if (shutdown.get() != null) {
            error = Errors.BROKER_NOT_AVAILABLE;
        }

        return new FindLeaderResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    private void handleFindLeaderResponse(FindLeaderResponseData response) {
        OptionalInt leaderIdOpt = optionalLeaderId(response.leaderId());
        if (handleNonMatchingResponseLeaderAndEpoch(response.leaderEpoch(), leaderIdOpt))
            return;

        // We only need to become a follower if the current leader is not elected
        leaderIdOpt.ifPresent(leaderId -> becomeFollower(leaderId, response.leaderEpoch()));
    }

    private boolean hasInconsistentLeader(int epoch, OptionalInt leaderId) {
        // Only elected leaders are sent in the request/response header, so if we have an elected
        // leaderId, it should be consistent with what is in the message.
        if (epoch != quorum.epoch()) {
            return false;
        } else {
            if (!quorum.leaderId().isPresent())
                return false;
            if (!leaderId.isPresent())
                return false;
            return !quorum.leaderId().equals(leaderId);
        }
    }

    private void becomeFollower(int epoch, OptionalInt leaderId) {
        if (leaderId.isPresent()) {
            becomeFollower(leaderId.getAsInt(), epoch);
        } else {
            becomeUnattachedFollower(epoch);
        }
    }

    private void updateConnectionState(RaftResponse.Inbound response, Errors error) {
        long currentTimeMs = electionTimer.currentTimeMs();
        ConnectionState connection = voterConnections.get(response.sourceId());

        if (quorum.isObserver() && response.data() instanceof FindLeaderResponseData) {
            findLeaderConnection.onResponse(response.requestId(), error, currentTimeMs);
        }

        connection.onResponse(response.requestId(), error, currentTimeMs);
    }

    private Errors responseError(RaftResponse.Inbound response) {
        final short errorCode;
        ApiMessage data = response.data();
        if (data instanceof FetchRecordsResponseData) {
            errorCode = ((FetchRecordsResponseData) data).errorCode();
        } else if (data instanceof AppendRecordsResponseData) {
            errorCode = ((AppendRecordsResponseData) data).errorCode();
        } else if (data instanceof FetchEndOffsetResponseData) {
            errorCode = ((FetchEndOffsetResponseData) data).errorCode();
        } else if (data instanceof VoteResponseData) {
            errorCode = ((VoteResponseData) data).errorCode();
        } else if (data instanceof BeginEpochResponseData) {
            errorCode = ((BeginEpochResponseData) data).errorCode();
        } else if (data instanceof EndEpochResponseData) {
            errorCode = ((EndEpochResponseData) data).errorCode();
        } else if (data instanceof FindLeaderResponseData) {
            errorCode = ((FindLeaderResponseData) data).errorCode();
        } else {
            throw new IllegalStateException("Received unexpected response " + response);
        }
        return Errors.forCode(errorCode);
    }

    private boolean handleNonMatchingResponseLeaderAndEpoch(int epoch, OptionalInt leaderId) {
        if (epoch < quorum.epoch()) {
            return true;
        } else if (epoch > quorum.epoch()) {
            // For any request type, if the response indicates a higher epoch, we bump our local
            // epoch and become a follower. Responses only include elected leaders.
            GracefulShutdown gracefulShutdown = shutdown.get();
            if (gracefulShutdown != null) {
                gracefulShutdown.onEpochUpdate(epoch);
            } else {
                becomeFollower(epoch, leaderId);
            }

            return true;
        } else if (hasInconsistentLeader(epoch, leaderId)) {
            throw new IllegalStateException("Received response with leader " + leaderId +
                    " which is inconsistent with current leader " + quorum.leaderId());
        }
        return false;
    }

    private void handleResponse(RaftResponse.Inbound response) {
        // The response epoch matches the local epoch, so we can handle the response
        ApiMessage responseData = response.data();
        if (responseData instanceof FetchRecordsResponseData) {
            handleFetchRecordsResponse((FetchRecordsResponseData) responseData);
        } else if (responseData instanceof AppendRecordsResponseData) {
            handleAppendRecordsResponse(response.requestId(), (AppendRecordsResponseData) responseData);
        } else if (responseData instanceof FetchEndOffsetResponseData) {
            handleFetchEndOffsetResponse((FetchEndOffsetResponseData) responseData);
        } else if (responseData instanceof VoteResponseData) {
            handleVoteResponse(response.sourceId(), (VoteResponseData) responseData);
        } else if (responseData instanceof BeginEpochResponseData) {
            handleBeginEpochResponse(response.sourceId(), (BeginEpochResponseData) responseData);
        } else if (responseData instanceof EndEpochResponseData) {
            handleEndEpochResponse((EndEpochResponseData) responseData);
        } else if (responseData instanceof FindLeaderResponseData) {
            handleFindLeaderResponse((FindLeaderResponseData) responseData);
        } else {
            throw new IllegalStateException("Received unexpected response " + response);
        }
    }

    private Optional<Exception> handleInvalidVoterOnlyRequest(int remoteNodeId, int requestEpoch) {
        if (quorum.isObserver()) {
            return Optional.of(Errors.INVALID_REQUEST.exception());
        } else if (!quorum.isVoter(remoteNodeId)) {
            return Optional.of(Errors.INVALID_REQUEST.exception());
        } else if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH.exception());
        } else if (shutdown.get() != null) {
            shutdown.get().onEpochUpdate(requestEpoch);
            return Optional.of(Errors.BROKER_NOT_AVAILABLE.exception());
        }

        // TODO: seems like a weird place to do this...
        if (requestEpoch > quorum.epoch()) {
            becomeUnattachedFollower(requestEpoch);
        }
        return Optional.empty();
    }

    private Optional<Exception> handleInvalidLeaderOnlyRequest(int requestEpoch) {
        if (quorum.isObserver()) {
            return Optional.of(new KafkaException("Observers are not allowed to receive requests"));
        } else if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH.exception());
        } else if (requestEpoch > quorum.epoch()) {
            // We cannot be the leader of an epoch we are not aware of
            return Optional.of(Errors.UNKNOWN_LEADER_EPOCH.exception());
        } else if (!quorum.isLeader()) {
            return Optional.of(Errors.NOT_LEADER_FOR_PARTITION.exception());
        } else if (shutdown.get() != null) {
            return Optional.of(Errors.BROKER_NOT_AVAILABLE.exception());
        }
        return Optional.empty();
    }

    private void handleRequest(RaftRequest.Inbound request) {
        ApiMessage requestData = request.data();
        final ApiMessage responseData;
        if (requestData instanceof FetchRecordsRequestData) {
            responseData = handleFetchRecordsRequest((FetchRecordsRequestData) requestData);
        } else if (requestData instanceof AppendRecordsRequestData) {
            responseData = handleAppendRecordsRequest((AppendRecordsRequestData) requestData);
        } else if (requestData instanceof FetchEndOffsetRequestData) {
            responseData = handleFetchEndOffsetRequest((FetchEndOffsetRequestData) requestData);
        } else if (requestData instanceof VoteRequestData) {
            responseData = handleVoteRequest((VoteRequestData) requestData);
        } else if (requestData instanceof BeginEpochRequestData) {
            responseData = handleBeginEpochRequest((BeginEpochRequestData) requestData);
        } else if (requestData instanceof EndEpochRequestData) {
            responseData = handleEndEpochRequest((EndEpochRequestData) requestData);
        } else if (requestData instanceof FindLeaderRequestData) {
            responseData = handleFindLeaderRequest();
        } else {
            throw new IllegalStateException("Unexpected request type " + requestData);
        }

        channel.send(new RaftResponse.Outbound(request.requestId(), responseData));
    }

    private void handleInboundMessage(RaftMessage message) {
        if (message instanceof RaftRequest.Inbound) {
            handleRequest((RaftRequest.Inbound) message);
        } else if (message instanceof RaftResponse.Inbound) {
            RaftResponse.Inbound response = (RaftResponse.Inbound) message;
            int sourceId = response.sourceId();

            // We should only be receiving responses from voters
            if (!quorum.isVoter(sourceId))
                throw new IllegalStateException("Unexpected response " + response + " from non-voter" + sourceId);

            Errors error = responseError(response);
            updateConnectionState(response, error);
            if (error == Errors.NONE)
                handleResponse(response);
        } else {
            throw new IllegalStateException("Unexpected message " + message);
        }
    }

    private OptionalInt maybeSendRequest(long currentTimeMs, int destinationId, Supplier<ApiMessage> requestData) {
        ConnectionState connection = voterConnections.get(destinationId);
        if (connection.isReady(currentTimeMs)) {
            int requestId = channel.newRequestId();
            channel.send(new RaftRequest.Outbound(requestId, requestData.get(), destinationId, currentTimeMs));
            connection.onRequestSent(requestId, electionTimer.currentTimeMs());
            return OptionalInt.of(requestId);
        }
        return OptionalInt.empty();
    }

    private EndEpochRequestData buildEndEpochRequest() {
        return new EndEpochRequestData()
                .setReplicaId(quorum.localId)
                .setLeaderId(quorum.leaderIdOrNil())
                .setLeaderEpoch(quorum.epoch());
    }

    private void maybeSendEndEpoch(long currentTimeMs) {
        for (Integer voterId : quorum.remoteVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildEndEpochRequest);
        }
    }

    private BeginEpochRequestData buildBeginEpochRequest() {
        EndOffset previousEpochEndOffset = log.previousEpoch()
                .flatMap(log::endOffsetForEpoch)
                .orElseThrow(() -> new IllegalStateException("Expected leader to have defined previous epoch"));
        return new BeginEpochRequestData()
                .setLeaderId(quorum.localId)
                .setLeaderEpoch(quorum.epoch())
                .setPreviousEpoch(previousEpochEndOffset.epoch)
                .setPreviousEpochEndOffset(previousEpochEndOffset.offset);
    }

    private void maybeSendBeginEpochToFollowers(long currentTimeMs, LeaderState state) {
        for (Integer followerId : state.nonEndorsingFollowers()) {
            maybeSendRequest(currentTimeMs, followerId, this::buildBeginEpochRequest);
        }
    }

    private VoteRequestData buildVoteRequest() {
        EndOffset endOffset = endOffset();
        return new VoteRequestData()
                .setLeaderEpoch(quorum.epoch())
                .setCandidateId(quorum.localId)
                .setLastEpoch(endOffset.epoch)
                .setLastEpochEndOffset(endOffset.offset);
    }

    private void maybeSendVoteRequestToVoters(long currentTimeMs, CandidateState state) {
        for (Integer voterId : state.remainingVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildVoteRequest);
        }
    }

    private FetchRecordsRequestData buildFetchRecordsRequest() {
        return new FetchRecordsRequestData()
                .setLeaderEpoch(quorum.epoch())
                .setReplicaId(quorum.localId)
                .setFetchOffset(log.endOffset());
    }

    private void maybeSendFetchRecords(long currentTimeMs, int leaderId) {
        maybeSendRequest(currentTimeMs, leaderId, this::buildFetchRecordsRequest);
    }

    private FetchEndOffsetRequestData buildFetchEndOffsetRequest() {
        return new FetchEndOffsetRequestData()
                .setLeaderEpoch(quorum.epoch())
                .setReplicaId(quorum.localId)
                .setLastEpoch(log.latestEpoch());
    }

    private void maybeSendFetchEndOffset(long currentTimeMs, int leaderId) {
        maybeSendRequest(currentTimeMs, leaderId, this::buildFetchEndOffsetRequest);
    }

    private FindLeaderRequestData buildFindLeaderRequest() {
        return new FindLeaderRequestData().setReplicaId(quorum.localId);
    }

    private void maybeSendFindLeader(long currentTimeMs) {
        // Find a ready member of the quorum to send FindLeader to. Any voter can receive the
        // FindLeader, but we only send one request at a time.
        if (quorum.isObserver() && findLeaderConnection.isReady(currentTimeMs)) {
            for (Map.Entry<Integer, ConnectionState> voterConnectionEntry : voterConnections.entrySet()) {
                int voterId = voterConnectionEntry.getKey();
                ConnectionState voterConnection = voterConnectionEntry.getValue();
                if (voterConnection.isReady(currentTimeMs)) {
                    OptionalInt requestIdOpt = maybeSendRequest(currentTimeMs, voterId, this::buildFindLeaderRequest);
                    requestIdOpt.ifPresent(requestId -> findLeaderConnection.onRequestSent(requestId, currentTimeMs));
                }
            }
        }
    }

    private void maybeSendRequests(long currentTimeMs) {
        quorum.visit(new QuorumState.VoidVisitor() {
            @Override
            public void ifFollower(FollowerState state) {
                if (!state.hasLeader()) {
                    maybeSendFindLeader(currentTimeMs);
                } else {
                    int leaderId = state.leaderId();
                    if (awaitingTruncation) {
                        maybeSendFetchEndOffset(currentTimeMs, leaderId);
                    } else {
                        maybeSendFetchRecords(currentTimeMs, leaderId);
                    }
                }
            }

            @Override
            public void ifLeader(LeaderState state) {
                maybeSendBeginEpochToFollowers(currentTimeMs, state);
            }

            @Override
            public void ifCandidate(CandidateState state) {
                maybeSendVoteRequestToVoters(currentTimeMs, state);
            }
        });
    }

    public boolean isRunning() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown == null || !gracefulShutdown.isFinished();
    }

    private void pollShutdown(GracefulShutdown shutdown) {
        // Graceful shutdown allows a leader or candidate to resign its leadership without
        // awaiting expiration of the election timeout. During shutdown, we no longer update
        // quorum state. All we do is check for epoch updates and try to send EndEpoch request
        // to finish our term. We consider the term finished if we are a follower or if one of
        // the remaining voters bumps the existing epoch.

        shutdown.timer.update();

        if (quorum.isFollower() || quorum.remoteVoters().isEmpty()) {
            // Shutdown immediately if we are a follower or we are the only voter
            shutdown.finished.set(true);
        } else if (!shutdown.isFinished()) {
            long currentTimeMs = shutdown.timer.currentTimeMs();
            maybeSendEndEpoch(currentTimeMs);

            List<RaftMessage> inboundMessages = channel.receive(shutdown.timer.remainingMs());
            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message);
        }
    }

    public void poll() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        if (gracefulShutdown != null) {
            pollShutdown(gracefulShutdown);
        } else {
            // This call is a bit annoying, but perhaps justifiable if we need to acquire a lock
            electionTimer.update();

            if (electionTimer.isExpired())
                becomeCandidate();

            clearTimedOutAppends(electionTimer.currentTimeMs());
            maybeSendRequests(electionTimer.currentTimeMs());
            maybeSendOrHandleAppendRequest(electionTimer.currentTimeMs());

            // TODO: Receive time needs to take into account backing off operations that still need doing
            List<RaftMessage> inboundMessages = channel.receive(electionTimer.remainingMs());
            electionTimer.update();

            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message);
        }
    }

    private AppendRecordsRequestData buildAppendRequest(Records records) {
        return new AppendRecordsRequestData()
                .setLeaderEpoch(quorum.epoch())
                .setRecords(RaftUtil.serializeRecords(records));
    }

    private void maybeSendOrHandleAppendRequest(long currentTimeMs) {
        PendingAppendRequest unsentAppend = unsentAppends.peek();
        if (unsentAppend == null)
            return;

        if (unsentAppend.isCancelled()) {
            unsentAppends.poll();
        }  else if (unsentAppend.isTimedOut(currentTimeMs)) {
            unsentAppends.poll();
            unsentAppend.fail(new TimeoutException());
        } else if (quorum.isLeader()) {
            unsentAppends.poll();
            int epoch = quorum.epoch();
            Long baseOffset = log.appendAsLeader(unsentAppend.records, epoch);
            unsentAppend.complete(new OffsetAndEpoch(baseOffset, epoch));
        } else if (quorum.isFollower()) {
            FollowerState followerState = quorum.followerStateOrThrow();
            if (followerState.hasLeader()) {
                unsentAppends.poll();
                AppendRecordsRequestData appendRequest = buildAppendRequest(unsentAppend.records);
                int appendRequestId = channel.newRequestId();
                channel.send(new RaftRequest.Outbound(appendRequestId, appendRequest,
                        followerState.leaderId(), unsentAppend.createTimeMs));
                sentAppends.put(appendRequestId, unsentAppend);
            }
        }
    }

    private void clearTimedOutAppends(long currentTimeMs) {
        Iterator<PendingAppendRequest> pendingAppendIter = sentAppends.values().iterator();
        while (pendingAppendIter.hasNext()) {
            PendingAppendRequest pendingAppend = pendingAppendIter.next();
            if (!pendingAppend.isTimedOut(currentTimeMs))
                break;
            pendingAppendIter.remove();
            pendingAppend.fail(new TimeoutException());
        }
    }

    /**
     * Append a set of records to the log. Acknowledgement of this
     *
     * @param records The records to write to the log.
     * @return The uncommitted base offset and epoch of the appended records
     */
    public Future<OffsetAndEpoch> append(Records records) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");

        CompletableFuture<OffsetAndEpoch> future = new CompletableFuture<>();
        unsentAppends.offer(new PendingAppendRequest(records, future, time.milliseconds(), requestTimeoutMs));
        return future;
    }

    /**
     * Read from the local log. This will only return records which have been committed to the quorum.
     * @param offsetAndEpoch The first offset to read from and the previous consumed epoch
     * @return A set of records beginning at the request offset
     */
    public Records read(OffsetAndEpoch offsetAndEpoch) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");

        Optional<EndOffset> endOffset = log.endOffsetForEpoch(offsetAndEpoch.epoch);
        if (!endOffset.isPresent() || offsetAndEpoch.offset > endOffset.get().offset)
            throw new LogTruncationException();

        OptionalLong highWatermark = quorum.highWatermark();
        if (highWatermark.isPresent()) {
            return log.read(offsetAndEpoch.offset, highWatermark.getAsLong());
        } else {
            // We are in the middle of an election or we have not yet discovered the leader
            return MemoryRecords.EMPTY;
        }
    }

    public void shutdown(int timeoutMs) {
        // TODO: Safe to access epoch? Need to reset connections to be able to send EndEpoch? Block until shutdown completes?
        shutdown.set(new GracefulShutdown(timeoutMs, quorum.epoch()));
        channel.wakeup();
    }

    private class GracefulShutdown {
        final int epoch;
        final Timer timer;
        final AtomicBoolean finished = new AtomicBoolean(false);

        public GracefulShutdown(int shutdownTimeoutMs, int epoch) {
            this.timer = time.timer(shutdownTimeoutMs);
            this.epoch = epoch;
        }

        public void onEpochUpdate(int epoch) {
            // Shutdown is complete once the epoch has been bumped, which indicates
            // that a new election has been started.

            if (epoch > this.epoch)
                finished.set(true);
        }

        public boolean isFinished() {
            return succeeded() || failed();
        }

        public boolean succeeded() {
            return finished.get();
        }

        public boolean failed() {
            return timer.isExpired();
        }

    }

    private static class ConnectionState {
        private enum State {
            AWAITING_REQUEST,
            BACKING_OFF,
            READY
        }

        private final int retryBackoffMs;
        private final int requestTimeoutMs;
        private State state = State.READY;
        private long lastSendTimeMs = 0L;
        private long lastFailTimeMs = 0L;
        private Optional<Long> inFlightRequestId = Optional.empty();

        public ConnectionState(int retryBackoffMs, int requestTimeoutMs) {
            this.retryBackoffMs = retryBackoffMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        private boolean isBackoffComplete(long timeMs) {
            return state == State.BACKING_OFF && timeMs >= lastFailTimeMs + retryBackoffMs;
        }

        private boolean hasRequestTimedOut(long timeMs) {
            return state == State.AWAITING_REQUEST && timeMs >= lastSendTimeMs + requestTimeoutMs;
        }

        boolean isReady(long timeMs) {
            if (isBackoffComplete(timeMs) || hasRequestTimedOut(timeMs)) {
                state = State.READY;
            }
            return state == State.READY;
        }

        void onResponseError(long requestId, long timeMs) {
            inFlightRequestId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == requestId) {
                    lastFailTimeMs = timeMs;
                    state = State.BACKING_OFF;
                    inFlightRequestId = Optional.empty();
                }
            });
        }

        void onResponseReceived(long requestId) {
            inFlightRequestId.ifPresent(inflightRequestId -> {
                if (inflightRequestId == requestId) {
                    state = State.READY;
                    inFlightRequestId = Optional.empty();
                }
            });
        }

        void onResponse(long requestId, Errors error, long timeMs) {
            if (error != Errors.NONE) {
                onResponseError(requestId, timeMs);
            } else {
                onResponseReceived(requestId);
            }
        }

        void onRequestSent(long requestId, long timeMs) {
            lastSendTimeMs = timeMs;
            inFlightRequestId = Optional.of(requestId);
            state = State.AWAITING_REQUEST;
        }

        /**
         * Ignore in-flight requests or backoff and become available immediately. This is used
         * when there is a state change which usually means in-flight requests are obsolete
         * and we need to send new requests.
         */
        void reset() {
            state = State.READY;
            inFlightRequestId = Optional.empty();
        }
    }

    private static class PendingAppendRequest {
        private final Records records;
        private final CompletableFuture<OffsetAndEpoch> future;
        private final long createTimeMs;
        private final long requestTimeoutMs;

        private PendingAppendRequest(Records records,
                                     CompletableFuture<OffsetAndEpoch> future,
                                     long createTimeMs,
                                     long requestTimeoutMs) {
            this.records = records;
            this.future = future;
            this.createTimeMs = createTimeMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public void complete(OffsetAndEpoch offsetAndEpoch) {
            future.complete(offsetAndEpoch);
        }

        public void fail(Exception e) {
            future.completeExceptionally(e);
        }

        public boolean isTimedOut(long currentTimeMs) {
            return currentTimeMs > createTimeMs + requestTimeoutMs;
        }

        public boolean isCancelled() {
            return future.isCancelled();
        }
    }

}
