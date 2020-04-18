package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A reduced functionality of a combination of transaction coordinator and group coordinator.
 * It provides basic event handling from KafkaProducer.Sender with transaction turned on.
 *
 * TODO: add log truncation support to this framework
 * TODO: add log tail deletion support to this framework
 */
class TransactionSimulationCoordinator {

    private final Map<TopicPartition, List<Record>> pendingPartitionData;
    private final Map<TopicPartition, Long> pendingOffsets;
    private boolean offsetsAddedToTxn = false;

    private long nextProducerId = 0L;

    public Map<TopicPartition, List<Record>> persistentPartitionData() {
        return persistentPartitionData;
    }

    public Map<TopicPartition, Long> committedOffsets() {
        return committedOffsets;
    }

    private final Map<TopicPartition, List<Record>> persistentPartitionData;
    private final Map<TopicPartition, Long> committedOffsets;

    private final MockClient networkClient;
    private final int throttleTimeMs = 10;

    TransactionSimulationCoordinator(MockClient networkClient) {
        this.networkClient = networkClient;
        this.pendingPartitionData = new HashMap<>();
        this.pendingOffsets = new HashMap<>();
        this.persistentPartitionData = new HashMap<>();
        this.committedOffsets = new HashMap<>();
    }

    void runOnce() {
        Queue<ClientRequest> incomingRequests = networkClient.requests();
        if (!incomingRequests.isEmpty()) {
            final AbstractResponse response;
            AbstractRequest nextRequest = incomingRequests.peek().requestBuilder().build();
            if (nextRequest instanceof FindCoordinatorRequest) {
                response = handleFindCoordinator((FindCoordinatorRequest) nextRequest, false);
            } else if (nextRequest instanceof InitProducerIdRequest) {
                response = handleInitProducerId((InitProducerIdRequest) nextRequest);
            } else if (nextRequest instanceof AddPartitionsToTxnRequest) {
                response = handleAddPartitionToTxn((AddPartitionsToTxnRequest) nextRequest);
            } else if (nextRequest instanceof AddOffsetsToTxnRequest) {
                response = handleAddOffsetsToTxn((AddOffsetsToTxnRequest) nextRequest);
            } else if (nextRequest instanceof TxnOffsetCommitRequest) {
                response = handleTxnCommit((TxnOffsetCommitRequest) nextRequest);
            } else if (nextRequest instanceof ProduceRequest) {
                response = handleProduce((ProduceRequest) nextRequest);
            } else if (nextRequest instanceof EndTxnRequest) {
                response = handleEndTxn((EndTxnRequest) nextRequest);
            } else {
                throw new IllegalArgumentException("Unknown request: " + nextRequest);
            }
            networkClient.respond(response);
        }
    }

    private FindCoordinatorResponse handleFindCoordinator(FindCoordinatorRequest request,
                                                          final boolean faultInject) {
        if (faultInject) {
            return new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
        } else {
            return new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setHost("localhost")
                    .setNodeId(0)
                    .setPort(2211)
            );
        }
    }

    private InitProducerIdResponse handleInitProducerId(InitProducerIdRequest request) {
        InitProducerIdResponse response = new InitProducerIdResponse(
            new InitProducerIdResponseData()
                .setProducerId(nextProducerId)
                .setProducerEpoch((short) (request.data.producerEpoch() + 1))
                .setErrorCode(Errors.NONE.code())
        );
        nextProducerId += 1;
        return response;
    }

    private AddPartitionsToTxnResponse handleAddPartitionToTxn(AddPartitionsToTxnRequest request) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        request.partitions().forEach(topicPartition ->
            errors.put(topicPartition, Errors.NONE)
        );
        return new AddPartitionsToTxnResponse(
            throttleTimeMs,
            errors
        );
    }

    private AddOffsetsToTxnResponse handleAddOffsetsToTxn(AddOffsetsToTxnRequest request) {
        offsetsAddedToTxn = true;
        return new AddOffsetsToTxnResponse(
           new AddOffsetsToTxnResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(throttleTimeMs)
        );
    }

    private AbstractResponse handleTxnCommit(TxnOffsetCommitRequest request) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        request.data.topics().forEach(topic -> topic.partitions().forEach(partition -> {
            TopicPartition key = new TopicPartition(topic.name(), partition.partitionIndex());
            if (offsetsAddedToTxn) {
                pendingOffsets.put(key, partition.committedOffset());
                errors.put(key, Errors.NONE);
            } else {
                errors.put(key, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
        }));
        return new TxnOffsetCommitResponse(
            throttleTimeMs,
            errors
        );
    }

    private AbstractResponse handleProduce(ProduceRequest request) {
        Map<TopicPartition, MemoryRecords> records = request.partitionRecordsOrFail();

        for (Map.Entry<TopicPartition, MemoryRecords> entry : records.entrySet()) {
            List<Record> sentRecords = pendingPartitionData.getOrDefault(entry.getKey(), new ArrayList<>());
            for (Record record : entry.getValue().records()) {
                sentRecords.add(record);
            }
            pendingPartitionData.put(entry.getKey(), sentRecords);
        }

        Map<TopicPartition, PartitionResponse> errors = new HashMap<>();
        records.forEach((topicPartition, record) -> errors.put(topicPartition, new PartitionResponse(Errors.NONE)));
        return new ProduceResponse(errors, throttleTimeMs);
    }

    private AbstractResponse handleEndTxn(EndTxnRequest request) {
        if (request.result().equals(TransactionResult.COMMIT)) {
            for (Map.Entry<TopicPartition, List<Record>> entry : pendingPartitionData.entrySet()) {
                List<Record> materializedRecords = persistentPartitionData.getOrDefault(entry.getKey(), new ArrayList<>());
                materializedRecords.addAll(entry.getValue());
                persistentPartitionData.put(entry.getKey(), materializedRecords);
            }

            committedOffsets.putAll(pendingOffsets);
        }
        pendingPartitionData.clear();
        pendingOffsets.clear();
        offsetsAddedToTxn = false;

        return new EndTxnResponse(
            new EndTxnResponseData()
            .setErrorCode(Errors.NONE.code())
        );
    }
}
