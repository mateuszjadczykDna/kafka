/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.client.ConsumerBuilder;
import kafka.tier.client.ProducerBuilder;
import kafka.tier.client.TierTopicConsumerBuilder;
import kafka.tier.client.TierTopicProducerBuilder;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.exceptions.TierMetadataRetryableException;
import kafka.tier.state.FileTierPartitionStateFactory;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionState.AppendResult;
import kafka.tier.state.TierPartitionStateFactory;
import kafka.tier.state.TierPartitionStatus;
import kafka.tier.topic.TierTopicAdmin;
import kafka.tier.topic.TierTopicPartitioner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Materializes the TierPartitionStatus from the Tier topic.
 * Maintains a mapping of TopicPartition to the materialized TierPartitionStatus.
 * Allows brokers to perform leader requests on Tier topic.
 */
public class TierTopicManager implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private static final int TOPIC_CREATION_BACKOFF_MS = 5000;
    // TODO(lucas), add a KafkaConfig if we choose to use the sparse index scheme
    private static final double POSITION_INDEX_SPARSITY = 0.001;
    private final String topicName;
    private final TierTopicManagerConfig config;
    private final TierPartitionStateFactory tierPartitionStateFactory;
    private final ConcurrentHashMap<TopicPartition, TierPartitionState> tierPartitionStates
            = new ConcurrentHashMap<>();
    private final TierTopicListeners resultListeners = new TierTopicListeners();
    private final TierTopicManagerCommitter committer;
    private final ConcurrentLinkedQueue<MigrationEntry> migrations = new ConcurrentLinkedQueue<>();
    private final TierTopicConsumerBuilder consumerBuilder;
    private final TierTopicProducerBuilder producerBuilder;
    private final AtomicLong heartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private TierTopicPartitioner partitioner;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(2);
    private Consumer<byte[], byte[]> primaryConsumer;
    private Consumer<byte[], byte[]> catchUpConsumer;
    private Producer<byte[], byte[]> producer;
    private volatile boolean ready = false;
    private KafkaThread committerThread;
    private KafkaThread managerThread;

    /**
     * Instantiate TierTopicManager. Once created, startup() must be called
     * in order to start normal operation.
     *
     * @param config                    TierTopicManagerConfig containing tiering configuration.
     * @param consumerBuilder           builder to create consumer instances.
     * @param producerBuilder           producer to create producer instances.
     * @param tierPartitionStateFactory TierPartitionState factory
     * @throws IOException on logdir write failures
     */
    TierTopicManager(TierTopicManagerConfig config,
                     TierTopicConsumerBuilder consumerBuilder,
                     TierTopicProducerBuilder producerBuilder,
                     TierPartitionStateFactory tierPartitionStateFactory) throws IOException {
        this.config = config;
        this.topicName = topicName(config.tierNamespace);
        this.committer = new TierTopicManagerCommitter(config, tierPartitionStates, shutdownInitiated);
        if (config.logDirs.size() > 1) {
            throw new UnsupportedOperationException("Multiple log.dirs detected. Tiered "
                    + "storage currently supports single logdir configuration.");
        }
        this.tierPartitionStateFactory = tierPartitionStateFactory;
        this.consumerBuilder = consumerBuilder;
        this.producerBuilder = producerBuilder;
    }

    /**
     * Primary public constructor for TierTopicManager.
     *
     * @param metrics kafka metrics to track TierTopicManager metrics
     * @param config  TierTopicManagerConfig containing tiering configuration.
     */
    public TierTopicManager(Metrics metrics,
                            TierTopicManagerConfig config) throws IOException {
        this(config,
                new ConsumerBuilder(config),
                new ProducerBuilder(config),
                new FileTierPartitionStateFactory(config.logDirs.get(0), POSITION_INDEX_SPARSITY));
        setupMetrics(metrics);
    }

    /**
     * Startup the tier topic manager.
     */
    public void startup() {
        managerThread = new KafkaThread("TierTopicManager", this, false);
        managerThread.start();
        committerThread = new KafkaThread("TierTopicManagerCommitter", committer, false);
        committerThread.start();
    }

    /**
     * Shutdown the tier topic manager.
     */
    public void shutdown() {
        shutdown.set(true);
        primaryConsumer.wakeup();
        if (catchUpConsumer != null) {
            catchUpConsumer.wakeup();
        }
        producer.close();
        try {
            shutdownInitiated.await();
        } catch (InterruptedException ie) {
            log.debug("shutdownInitiated latch count reached zero. Shutdown called.");
        }
    }

    /**
     * Generate the tier topic name, namespaced if tierNamespace is non-empty.
     *
     * @param tierNamespace Tier Topic namespace for placing tier topic on external cluster.
     * @return The topic name.
     */
    public static String topicName(String tierNamespace) {
        return tierNamespace != null && !tierNamespace.isEmpty()
                ? String.format("%s_%s", Topic.TIER_TOPIC_NAME, tierNamespace)
                : Topic.TIER_TOPIC_NAME;
    }

    /**
     * Write an AbstractTierMetadata to the Tier Topic, returning a
     * CompletableFuture that tracks the result of the materialization after the
     * message has been read from the tier topic, allowing the sender to determine
     * whether the write was fenced, or the send failed.
     *
     * @param entry the tier topic entry to be written to the tier topic.
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    public CompletableFuture<AppendResult> addMetadata(AbstractTierMetadata entry) throws IllegalAccessException {
        ensureReady();

        final TopicPartition tp = new TopicPartition(entry.topic(), entry.partition());
        // track this entry's materialization
        final CompletableFuture<AppendResult> result = resultListeners.addTracked(tp, entry);
        producer.send(new ProducerRecord<>(topicName, partitioner.partitionId(tp),
                        entry.serializeKey(),
                        entry.serializeValue()),
                (recordMetadata, exception) -> {
                    if (exception != null) {
                        if (retryable(exception)) {
                            result.completeExceptionally(
                                    new TierMetadataRetryableException(
                                            "Retryable exception sending tier metadata.",
                                            exception));
                        } else {
                            result.completeExceptionally(
                                    new TierMetadataFatalException(
                                            "Fatal exception sending tier metadata.",
                                            exception));
                        }
                        resultListeners.getAndRemoveTracked(tp, entry);
                    }
                });
        return result;
    }

    /**
     * Return the TierPartitionState for a given topic partition, if one is being followed.
     *
     * @param topicPartition tiered topic partition
     * @return TierPartitionState for this partition.
     */
    public TierPartitionState getPartitionState(TopicPartition topicPartition) {
        return tierPartitionStates.get(topicPartition);
    }

    /**
     * Performs a write to the tier topic to attempt to become leader for the tiered topic partition.
     *
     * @param topicPartition the topic partition for which the sender wishes to become the archive leader.
     * @param tierEpoch      the archiver epoch
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    public CompletableFuture<AppendResult> becomeArchiver(TopicPartition topicPartition,
                                                          int tierEpoch)
            throws IllegalAccessException {
        ensureReady();
        // Generate a unique ID in order to track the leader request under scenarios
        // where we maintain the same leader ID.
        // This is possible when there is a single broker, and is primarily for defensive reasons.
        final UUID messageId = UUID.randomUUID();
        final TierTopicInitLeader initRecord =
                new TierTopicInitLeader(topicPartition.topic(), topicPartition.partition(),
                        tierEpoch, messageId, config.brokerId);

        return addMetadata(initRecord);
    }

    /**
     * Return whether TierTopicManager is ready to accept writes.
     *
     * @return boolean
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * tier topic manager work loop
     */
    public void run() {
        try {
            while (!ready && !shutdown.get()) {
                if (TierTopicAdmin.ensureTopicCreated(config.bootstrapServers, topicName,
                        config.numPartitions, config.replicationFactor)) {
                    becomeReady();
                    final int producerPartitions = producer.partitionsFor(topicName).size();
                    if (producerPartitions != config.numPartitions) {
                        log.error("Number of partitions {} on tier topic: {} " +
                                        "does not match the number of partitions configured {}.",
                                producerPartitions, topicName, config.numPartitions);
                        Exit.exit(1);
                    }
                } else {
                    log.warn("Failed to ensure tier topic has been created. Retrying in {}",
                            TOPIC_CREATION_BACKOFF_MS);
                    Thread.sleep(TOPIC_CREATION_BACKOFF_MS);
                }
            }
            while (!shutdown.get()) {
                doWork();
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!shutdown.get()) {
                throw e;
            }
        } catch (IOException io) {
            log.error("Unrecoverable IOException in TierTopicManager", io);
            Exit.exit(1);
        } catch (AuthenticationException | AuthorizationException e) {
            log.error("Unrecoverable authentication or authorization issue in TierTopicManager", e);
            Exit.exit(1);
        } catch (KafkaException | IllegalStateException e) {
            log.error("Unrecoverable error in work cycle", e);
            Exit.exit(1);
        } catch (InterruptedException ie) {
            log.error("Topic manager interrupted", ie);
            Exit.exit(1);
        } catch (TierMetadataDeserializationException de) {
            log.error("Tier topic: deserialization error encountered materializing tier topic.",
                    de);
            Exit.exit(1);
        } finally {
            primaryConsumer.close();
            if (catchUpConsumer != null) {
                catchUpConsumer.close();
            }
            committer.shutdown();
            shutdownInitiated.countDown();
        }
    }

    /**
     * @return boolean denoting whether catch up consumer is currently materializing the tier topic.
     */
    public boolean catchingUp() {
        return catchUpConsumer != null;
    }

    /**
     * Adds partitions to the migration queue to be immigrated.
     *
     * @param partitions
     */
    public void immigratePartitions(List<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            tierPartitionStates.computeIfAbsent(tp, tierPartitionStateFactory::newTierPartition);
            migrations.add(new MigrationEntry(tp, MigrationEntry.Type.IMMIGRATION));
        }
    }

    /**
     * Adds partitions to the migration queue to be emigrated.
     *
     * @param partitions
     */
    public void emigratePartitions(List<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            migrations.add(new MigrationEntry(tp, MigrationEntry.Type.EMIGRATION));
        }
    }

    /**
     * work cycle
     */
    boolean doWork() throws TierMetadataDeserializationException, IOException {
        processMigrations();
        checkCatchingUpComplete();
        final boolean primaryProcessed = pollConsumer(primaryConsumer, TierPartitionStatus.ONLINE);
        final boolean catchUpProcessed = catchUpConsumer != null
                && pollConsumer(catchUpConsumer, TierPartitionStatus.CATCHUP);

        heartbeat.set(System.currentTimeMillis());
        return primaryProcessed || catchUpProcessed;
    }

    /**
     * Ensure tier topic has been created and setup the backing consumer
     * and producer before signalling ready.
     */
    void becomeReady() {
        primaryConsumer = consumerBuilder.setupConsumer(committer, topicName, "primary");
        primaryConsumer.assign(partitions());
        for (Map.Entry<Integer, Long> entry : committer.positions().entrySet()) {
            primaryConsumer.seek(new TopicPartition(topicName, entry.getKey()), entry.getValue());
        }

        producer = producerBuilder.setupProducer();
        partitioner = new TierTopicPartitioner(config.numPartitions);
        ready = true;
    }


    TierTopicManagerCommitter committer() {
        return committer;
    }

    /**
     * @return All of the partitions for the Tier Topic
     */
    private Collection<TopicPartition> partitions() {
        return IntStream
                .range(0, config.numPartitions)
                .mapToObj(part -> new TopicPartition(topicName, part))
                .collect(Collectors.toList());
    }

    /**
     * Generate the tier topic partitions containing data for tiered partitions.
     *
     * @param tieredPartitions partitions that have been tiered
     * @return The partitions on the Tier Topic containing data for tieredPartitions
     */
    private Collection<TopicPartition> requiredPartitions(Collection<TopicPartition> tieredPartitions) {
        return tieredPartitions
                .stream()
                .map(tp -> new TopicPartition(topicName, partitioner.partitionId(tp)))
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Computes the offset distance between the positions of two consumers
     *
     * @return Optional distance, Optional.empty if no partitions are assigned to catch up consumer.
     */
    private Long catchUpConsumerLag() {
        Set<TopicPartition> catchUpAssignment = catchUpConsumer.assignment();
        return primaryConsumer
                .assignment()
                .stream()
                .filter(catchUpAssignment::contains)
                .map(tp -> Math.max(0, primaryConsumer.position(tp) - catchUpConsumer.position(tp)))
                .reduce(Long::sum)
                .orElse(0L);
    }

    /**
     * Checks whether catch up consumer has caught up to primary consumer.
     * If caught up, shuts down the catch up consumer.
     */
    private void checkCatchingUpComplete() {
        if (catchingUp() && catchUpConsumerLag() == 0) {
            completeCatchUp();
        }
    }

    /**
     * When all tier partition states have caught up, switch consumers.
     */
    private void completeCatchUp() {
        log.info("Completed adding partitions. Setting states online. Switching catchup consumer to primary consumer.");
        for (TierPartitionState tierPartitionState : tierPartitionStates.values()) {
            try {
                if (tierPartitionState.status() == TierPartitionStatus.CATCHUP) {
                    tierPartitionState.targetStatus(TierPartitionStatus.ONLINE);
                }
            } catch (IOException ioe) {
                // TODO: handle as part of https://confluentinc.atlassian.net/browse/CPKAFKA-1764
                // and JBOD support.
                ioe.printStackTrace();
            }
        }
        catchUpConsumer.close();
        catchUpConsumer = null;
    }

    /**
     * Drains the migration queue of entries if a catch up consumer is not already materializing.
     * For immigrating partitions, sets TierPartition state to CATCHUP
     * and instantiates the catch up consumer, assigning it the Tier Topic partitions
     * containing data for the immigrating partitions.
     */
    private void processMigrations() {
        if (!catchingUp() && !migrations.isEmpty()) {
            HashSet<TopicPartition> migrationCandidates = pollMigrations();
            HashSet<TopicPartition> transitioned = new HashSet<>();
            if (!migrationCandidates.isEmpty()) {
                for (TopicPartition tp : migrationCandidates) {
                    try {
                        log.debug("Adding {} to catchingUp partition states.", tp);
                        TierPartitionState tps = tierPartitionStates.get(tp);
                        switch (tps.status()) {
                            case INIT:
                                tps.targetStatus(TierPartitionStatus.CATCHUP);
                                transitioned.add(tp);
                                break;
                            case ONLINE:
                                log.warn("Tiered partition {} was already being tracked in status {}. "
                                        + "This may be indicative of a bug or churning in leader "
                                        + "election.", tp, tps.status());
                                break;
                            case DISK_OFFLINE:
                                log.warn("Tiered partition {} disk is OFFLINE. Ignoring "
                                        + "materialization request", tp);
                                break;
                            case CATCHUP:
                                throw new IllegalStateException(
                                        "Tiered partition in state CATCHUP, when no catchUpConsumer is running.");
                        }
                    } catch (IOException ioe) {
                        // TODO: handle as part of https://confluentinc.atlassian.net/browse/CPKAFKA-1764
                        // and JBOD support.
                        ioe.printStackTrace();
                    }
                }

                if (!transitioned.isEmpty()) {
                    catchUpConsumer = consumerBuilder.setupConsumer(committer, topicName, "catchup");
                    catchUpConsumer.assign(requiredPartitions(transitioned));

                    log.info("Seeking consumer to beginning.");

                    // TODO: upon adding snapshot support, we should seek to the earliest point
                    // required to restore all required snapshots
                    catchUpConsumer.seekToBeginning(catchUpConsumer.assignment());
                }
            }
        }
    }

    /**
     * polls the migration queue, removing emigrated partitions
     * and returning a set of the added partitions.
     *
     * @return HashSet containing immigrated partitions.
     */
    private HashSet<TopicPartition> pollMigrations() {
        HashSet<TopicPartition> added = new HashSet<>();
        while (!migrations.isEmpty()) {
            MigrationEntry entry = migrations.poll();
            switch (entry.type) {
                case IMMIGRATION:
                    added.add(entry.topicPartition);
                    break;
                case EMIGRATION:
                    tierPartitionStates.remove(entry.topicPartition);
                    // there is no need to catch up to a partition
                    // that has been emigrated prior to being processed
                    added.remove(entry.topicPartition);
                    resultListeners.remove(entry.topicPartition);
                    break;
            }
        }
        return added;
    }


    /**
     * Poll a consumer, materializing Tier Topic entries to TierPartition state.
     *
     * @param consumer      the consumer to poll
     * @param requiredState The TierPartition must be in this state or else the metadata will be ignored.
     * @return boolean for whether any messages were processed
     * @throws IOException if error occurred writing to pier partition state/logdir.
     */
    private boolean pollConsumer(Consumer<byte[], byte[]> consumer,
                                 TierPartitionStatus requiredState) throws IOException {
        boolean processedMessages = false;
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(config.pollDuration)) {
            final Optional<AbstractTierMetadata> entry =
                    AbstractTierMetadata.deserialize(record.key(), record.value());
            if (entry.isPresent()) {
                processEntry(entry.get(), requiredState);
                committer.updatePosition(record.partition(), record.offset() + 1);
                processedMessages = true;
            }
        }
        return processedMessages;
    }

    /**
     * Sanity check to ensure TierTopicManager is ready before performing operations.
     *
     * @throws IllegalAccessException
     */
    private void ensureReady() throws IllegalAccessException {
        if (!ready) {
            throw new IllegalAccessException("Tier Topic manager is not ready.");
        }
    }

    /**
     * Setup metrics for the tier topic manager.
     */
    private void setupMetrics(Metrics metrics) {
        metrics.addMetric(new MetricName("heartbeat",
                        "kafka.tier",
                        "Time since last heartbeat in milliseconds.",
                        new java.util.HashMap<>()),
                (MetricConfig config, long now) -> now - heartbeat.get());
    }

    /**
     * Materialize a tier topic entry into the corresponding tier partition status.
     *
     * @param entry         the tier topic entry read from the tier topic.
     * @param requiredState TierPartitionState must be in this status in order to modify it.
     *                      Otherwise the entry will be ignored.
     */
    private void processEntry(AbstractTierMetadata entry, TierPartitionStatus requiredState) throws IOException {
        final TopicPartition tp = new TopicPartition(entry.topic(), entry.partition());
        final TierPartitionState tierPartitionState = getPartitionState(tp);
        if (tierPartitionState == null) {
            return;
        }

        if (tierPartitionState.status() == requiredState) {
            final AppendResult result = tierPartitionState.append(entry);
            log.debug("Read entry {}, append result {}", entry, result);
            // signal completion of this tier topic entry if this topic manager was the sender
            resultListeners.getAndRemoveTracked(tp, entry)
                    .ifPresent(c -> c.complete(result));
        } else {
            log.debug("TierPartitionState {} not in required state {}. Ignoring metadata {}.",
                    tp, requiredState, entry);
        }
    }

    /**
     * Determine whether tiering is retryable or whether hard exit should occur
     *
     * @param e The exception
     * @return true if retryable, false otherwise.
     */
    private static boolean retryable(Exception e) {
        return e instanceof RetriableException;
    }

    private static class MigrationEntry {
        public enum Type {
            // supplied tier partition has been added to broker
            IMMIGRATION,
            // supplied tier partition has been removed from the broker
            EMIGRATION
        }

        public final TopicPartition topicPartition;
        public final Type type;

        MigrationEntry(TopicPartition topicPartition, Type type) {
            this.topicPartition = topicPartition;
            this.type = type;
        }
    }

    /**
     * Class to track outstanding requests and signal back to the TierTopicManager
     * user when their metadata requests have been read and materialized.
     */
    private static class TierTopicListeners {
        private final ConcurrentHashMap<TopicPartition, Entry> results = new ConcurrentHashMap<>();

        /**
         * Checks whether a given tier index entry is being tracked. If so,
         * returns a CompletableFuture to be completed to signal back to the sender.
         *
         * @param tp    tiered topic partition
         * @param entry tier index topic entry we are trying to complete
         * @return CompletableFuture for this index entry if one exists.
         */
        Optional<CompletableFuture<AppendResult>>
        getAndRemoveTracked(TopicPartition tp, AbstractTierMetadata entry) {
            final Entry complete = results.get(tp);
            if (complete != null && complete.key.equals(listenerKey(entry))) {
                results.remove(tp, complete);
                return Optional.of(complete.future);
            }
            return Optional.empty();
        }

        /**
         * Track a tier topic index entry's materialization into the tier topic.
         * If an index entry is already being tracked, then we exceptionally
         * complete the existing future before adding the new entry and future.
         *
         * @param tp    tiered topic partition
         * @param entry tier index topic entry to track materialization of.
         * @return future that will be completed when the entry has been materialized.
         */
        CompletableFuture<AppendResult> addTracked(TopicPartition tp,
                                                   AbstractTierMetadata entry) {
            final CompletableFuture<AppendResult> result = new CompletableFuture<>();
            final Entry complete = new Entry(listenerKey(entry), result);
            final Entry found = results.get(tp);
            if (found != null) {
                found.future.completeExceptionally(
                        new TierMetadataFatalException(
                                "A new index entry is being tracked for this topic partition"
                                        + ", obsoleting this request."));
            }
            results.put(tp, complete);
            return result;
        }

        /**
         * Stop tracking this partition after partition emigration
         *
         * @param tp topic partition.
         */
        void remove(TopicPartition tp) {
            final Entry found = results.get(tp);
            if (found != null) {
                found.future.completeExceptionally(new TierMetadataFatalException("TierPartitionState has"
                        + " been immigrated by the topic manager."));
                results.remove(tp, found);
            }
        }

        private static class Entry {
            public final TierMetadataListener key;
            public final CompletableFuture<AppendResult> future;

            Entry(TierMetadataListener key, CompletableFuture<AppendResult> future) {
                this.key = key;
                this.future = future;
            }
        }

        /**
         * Select a subset of the data in the tier index entry for use in tracking
         * the result of materialization. Reduces memory consumption vs tracking the entire
         * index entry.
         *
         * @return The key.
         */
        TierMetadataListener listenerKey(AbstractTierMetadata message) {
            if (message instanceof TierObjectMetadata) {
                TierObjectMetadata metadata = (TierObjectMetadata) message;
                return new TierObjectMetadataListener(metadata.topic(), metadata.partition(),
                        metadata.tierEpoch(), metadata.startOffset(),
                        metadata.endOffsetDelta());
            } else if (message instanceof TierTopicInitLeader) {
                TierTopicInitLeader initLeader = (TierTopicInitLeader) message;
                return new TierInitLeaderListener(initLeader.messageId());
            } else {
                throw new IllegalArgumentException(
                        "Tier topic message type unsupported in metadata listener "
                                + message.getClass().getName());
            }
        }

        interface TierMetadataListener {
        }

        class TierObjectMetadataListener implements TierMetadataListener {
            final private String topic;
            final private int partition;
            final private int tierEpoch;
            final private long startOffset;
            final private int endOffsetDelta;

            TierObjectMetadataListener(String topic, int partition, int tierEpoch,
                                       long startOffset, int endOffsetDelta) {
                this.topic = topic;
                this.partition = partition;
                this.tierEpoch = tierEpoch;
                this.startOffset = startOffset;
                this.endOffsetDelta = endOffsetDelta;
            }

            public int hashCode() {
                return Objects.hash(topic, partition, tierEpoch,
                        startOffset, endOffsetDelta);
            }

            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                TierObjectMetadataListener that = (TierObjectMetadataListener) o;
                return Objects.equals(topic, that.topic)
                        && Objects.equals(partition, that.partition)
                        && Objects.equals(tierEpoch, that.tierEpoch)
                        && Objects.equals(startOffset, that.startOffset)
                        && Objects.equals(endOffsetDelta, that.endOffsetDelta);
            }
        }

        class TierInitLeaderListener implements TierMetadataListener {
            final private UUID messageId;

            TierInitLeaderListener(UUID messageId) {
                this.messageId = messageId;
            }

            public int hashCode() {
                return Objects.hash(messageId);
            }

            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                TierInitLeaderListener that = (TierInitLeaderListener) o;
                return Objects.equals(messageId, that.messageId);
            }
        }

    }
}