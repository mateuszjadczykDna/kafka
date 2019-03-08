/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.DelayedOperationKey;
import kafka.server.TierFetcherOperationKey;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TierFetcher {
    final TierFetcherMetrics tierFetcherMetrics;
    private final Logger logger;
    private final TierObjectStore tierObjectStore;
    private final ConcurrentHashMap<UUID, PendingFetch> inFlight = new ConcurrentHashMap<>();
    private final ExecutorService executorService;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public TierFetcher(TierFetcherConfig tierFetcherConfig,
                       TierObjectStore tierObjectStore,
                       Metrics metrics,
                       LogContext logContext) {
        this.tierObjectStore = tierObjectStore;
        this.logger = logContext.logger(TierFetcher.class);
        this.tierFetcherMetrics = new TierFetcherMetrics(metrics);
        this.executorService = Executors.newFixedThreadPool(tierFetcherConfig.numFetchThreads);
    }

    TierFetcher(TierObjectStore tierObjectStore, Metrics metrics) {
        this(new TierFetcherConfig(), tierObjectStore, metrics, new LogContext());
    }

    /**
     * Seal the TierFetcher from accepting new fetches, and cancel all in-progress fetches. The
     * fetched data is not removed from the TierFetcher and is still accessible by requestId.
     */
    public void close() {
        logger.info("Closing TierFetcher");
        if (stopped.compareAndSet(false, true)) {
            for (UUID requestId : inFlight.keySet()) {
                this.cancel(requestId);
            }
            executorService.shutdownNow();
        }
    }

    /**
     * Query the status of a fetch. If `isComplete()` returns true, the calling thread can call
     * getFetchResultsAndRemove without blocking. If `isComplete()` returns false, the user must
     * poll isComplete at some point in the future, or initiate a best-effort cancellation using
     * `cancel()` in order to avoid block in `getFetchResultsAndRemove()`. If `isComplete()` returns
     * Optional.empty(), then the requestId is unknown to the TierFetcher.
     */
    public Optional<Boolean> isComplete(UUID requestId) {
        final PendingFetch fetchRequest = inFlight.get(requestId);
        if (fetchRequest != null) {
            if (fetchRequest.isComplete())
                return Optional.of(true);
            else
                return Optional.of(false);
        } else {
            return Optional.empty();
        }
    }

    OptionalInt bytesFetched(UUID requestId) {
        final PendingFetch fetchRequest = inFlight.get(requestId);
        if (fetchRequest != null)
            return OptionalInt.of(fetchRequest.bytesFetched());
        else
            return OptionalInt.empty();
    }

    /**
     * Early cancellation for a fetch. Does not remove the fetch from the TierFetcher.
     */
    public void cancel(UUID requestId) {
        final PendingFetch fetchRequest = inFlight.get(requestId);
        if (fetchRequest != null)
            fetchRequest.cancel();
    }

    /**
     * Early cancellation and removal for a fetch. Removes the fetch from the TierFetcher
     */
    private Optional<PendingFetch> cancelAndRemove(UUID requestId) {
        final PendingFetch fetchRequest = inFlight.remove(requestId);
        tierFetcherMetrics.setNumInFlight(inFlight.mappingCount());
        if (fetchRequest != null) {
            fetchRequest.cancel();
            return Optional.of(fetchRequest);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Removes and cancels the in-progress fetch for the given requestId.
     * This method will block the caller if the fetch is still in-progress. Use `cancel()` to cancel
     * early and `isComplete()` to check if this method can be called without blocking the caller.
     */
    public Optional<Map<TopicPartition, TierFetchResult>> getFetchResultsAndRemove(UUID requestId) {
        final Optional<PendingFetch> optFetchRequest = cancelAndRemove(requestId);
        return optFetchRequest.map(pendingFetch -> {
            Map<TopicPartition, TierFetchResult> fetchResult = pendingFetch.finish();
            final int totalBytesRead = fetchResult
                    .values()
                    .stream()
                    .mapToInt(value -> value.records.sizeInBytes())
                    .sum();
            tierFetcherMetrics.recordBytesFetched(totalBytesRead);
            return fetchResult;
        });
    }

    /**
     * Execute a read for a single partition from Tiered Storage.
     * The provided UUID can be used at any time to cancel the in-progress fetch and retrieve
     * any data fetched. fetchCompletionCallback will be called with the
     * DelayedOperationKey of the completed fetch.
     * <p>
     * Returns a list of TierFetcherOperationKey to be used when registering a DelayedOperation
     * which depends on this fetch.
     */
    public List<DelayedOperationKey> fetch(UUID requestId,
                                           List<TierFetchMetadata> tierFetchMetadataList,
                                           Consumer<DelayedOperationKey> fetchCompletionCallback) {
        if (!tierFetchMetadataList.isEmpty()) {
            // For now, we only fetch the first requested partition
            // This is subject to change in the future.
            final TierFetchMetadata firstFetchMetadata = tierFetchMetadataList.get(0);
            final List<TopicPartition> ignoredTopicPartitions =
                    tierFetchMetadataList.subList(1, tierFetchMetadataList.size())
                            .stream()
                            .map(tierFetchMetadata -> tierFetchMetadata.segmentMetadata().topicPartition())
                            .collect(Collectors.toList());

            if (firstFetchMetadata == null) {
                throw new IllegalStateException("No TierFetchMetadata supplied, cannot start fetch");
            } else if (!stopped.get()) {
                final DelayedOperationKey firstFetchDelayedOperationKey =
                        new TierFetcherOperationKey(firstFetchMetadata.segmentMetadata().topicPartition(),
                                requestId);

                logger.debug("Fetching " + firstFetchMetadata.segmentMetadata().topicPartition() + " "
                        + "from tiered storage");
                final TierObjectMetadata tierObjectMetadata = firstFetchMetadata.segmentMetadata();
                final long targetOffset = firstFetchMetadata.fetchStartOffset();
                final int maxBytes = firstFetchMetadata.maxBytes();
                final long maxOffset = OptionConverters.toJava(firstFetchMetadata.maxOffset()).map(v -> (Long) v).orElse(Long.MAX_VALUE);
                final CancellationContext cancellationContext = CancellationContext.newContext();
                final PendingFetch newFetch = new PendingFetch(cancellationContext,
                        tierObjectStore, tierObjectMetadata,
                        ignored -> fetchCompletionCallback.accept(firstFetchDelayedOperationKey),
                        targetOffset, maxBytes, maxOffset,
                        ignoredTopicPartitions);
                inFlight.put(requestId, newFetch);
                tierFetcherMetrics.setNumInFlight(inFlight.mappingCount());
                executorService.execute(newFetch);

                // Since we're only going to fetch the first topic partition, only return the
                // TierFetcherOperationKey associated with the first topic partition.
                return new ArrayList<>(Collections.singletonList(firstFetchDelayedOperationKey));
            } else {
                throw new IllegalStateException("TierFetcher is shutting down, request " + requestId +
                        " was not scheduled");
            }
        } else {
            throw new IllegalStateException("No TierFetchMetadata supplied to TierFetcher fetch "
                    + "request");
        }
    }
}
