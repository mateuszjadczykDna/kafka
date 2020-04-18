package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.TransactionManager.RequestType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test tries to test out the EOS robustness on the client side. It features a {@link TransactionSimulationCoordinator}
 * which handles the incoming transactional produce/metadata requests and gives feedback through an underlying client.
 */
public class TransactionEventSimulationTest {

    private Logger log = LoggerFactory.getLogger(TransactionEventSimulationTest.class);

    private final int numPartitions = 3;
    private TransactionManager transactionManager;
    private TransactionSimulationCoordinator transactionCoordinator;
    private Sender sender;
    private Map<Integer, Long> offsets;
    private final LogContext logContext = new LogContext();

    // What is the expected state guarantee
    // producer id, epoch, partitions in txn, partition data offset
    Map<RequestType, List<RequestType>> artificialTransitions;

    private final MockTime time = new MockTime();

    private ProducerMetadata metadata = new ProducerMetadata(0, Long.MAX_VALUE, 10,
        new LogContext(), new ClusterResourceListeners(), time);
    private MockClient client = new MockClient(time, metadata);

    @Before
    public void setup() {
        // The external transition is made only when there is no more enqueued request to be processed.
        artificialTransitions = mkMap(
            mkEntry(RequestType.FIND_COORDINATOR,
                Arrays.asList(RequestType.FIND_COORDINATOR, RequestType.INIT_PRODUCER_ID)),
            mkEntry(RequestType.INIT_PRODUCER_ID,
                Arrays.asList(RequestType.ADD_PARTITIONS, RequestType.FIND_COORDINATOR)),
            mkEntry(RequestType.ADD_PARTITIONS,
                Arrays.asList(RequestType.ADD_PARTITIONS, RequestType.COMMIT_TXN, RequestType.ABORT_TXN)),
            mkEntry(RequestType.COMMIT_TXN,
                Arrays.asList(RequestType.ADD_PARTITIONS, RequestType.INIT_PRODUCER_ID)),
            mkEntry(RequestType.ABORT_TXN,
                Arrays.asList(RequestType.ADD_PARTITIONS, RequestType.INIT_PRODUCER_ID))
        );

        offsets = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            offsets.put(i, 0L);
        }

        transactionManager = new TransactionManager(logContext, "txn-id",
            100, 0L, new ApiVersions());

        transactionCoordinator = new TransactionSimulationCoordinator(client);

    }

    @Test
    public void simulateTxnEvents() throws InterruptedException {
        Random random = new Random(10);
        Node brokerNode = new Node(0, "localhost", 2211);

        RecordAccumulator accumulator = new RecordAccumulator(logContext, 100, CompressionType.GZIP,
            0, 0L, 10, new Metrics(), "accumulator", time, new ApiVersions(), transactionManager,
            new BufferPool(1000, 100, new Metrics(), time, "producer-internal-metrics"));

        metadata.add("topic", time.milliseconds());
        metadata.update(metadata.newMetadataRequestAndVersion(time.milliseconds()).requestVersion,
            TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 2)), true, time.milliseconds());
        sender = new Sender(logContext, client, metadata, accumulator, false, 100, (short) 1,
            1, new SenderMetricsRegistry(new Metrics()), time, 100, 10, transactionManager, new ApiVersions());

        transactionManager.initializeTransactions();
        sender.runOnce();
        resolvePendingRequests();
        final int numTransactions = 100;

        TopicPartition key = new TopicPartition("topic", 0);

        for (int i = 0; i < numTransactions; i++) {
            transactionManager.beginTransaction();
            transactionManager.maybeAddPartitionToTransaction(key);
            accumulator.append(key, 0L, new byte[1], new byte[1], Record.EMPTY_HEADERS, null, 0, false, time.milliseconds());
            transactionManager.sendOffsetsToTransaction(Collections.singletonMap(key, new OffsetAndMetadata(numTransactions)), new ConsumerGroupMetadata("group"));
            transactionManager.beginCommit();
            System.out.println("Run " + i + " times");

            resolvePendingRequests();
        }

        assertTrue(transactionCoordinator.persistentPartitionData().containsKey(key));
        assertEquals(numTransactions, transactionCoordinator.persistentPartitionData().get(key).size());
        assertTrue(transactionCoordinator.committedOffsets().containsKey(key));
        assertEquals((long) numTransactions, (long) transactionCoordinator.committedOffsets().get(key));
    }

    private void resolvePendingRequests() {
        while (!client.requests().isEmpty() || transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION) == null || !transactionManager.isReady()) {
            transactionCoordinator.runOnce();
            System.out.println("Sender run once");
            sender.runOnce();
        }
    }
}
