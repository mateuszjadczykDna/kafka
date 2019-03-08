/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetPosition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class PendingFetch implements Runnable {
    private final CancellationContext cancellationContext;
    private final TierObjectStore tierObjectStore;
    private final TierObjectMetadata tierObjectMetadata;
    private final Consumer<Boolean> fetchCompletionCallback;
    private final long targetOffset;
    private final int maxBytes;
    private final long maxOffset;
    private final List<TopicPartition> ignoredTopicPartitions;

    private int bytesFetched = 0;

    private final CompletableFuture<MemoryRecords> transferPromise;
    private Exception exception;

    PendingFetch(CancellationContext cancellationContext,
                 TierObjectStore tierObjectStore,
                 TierObjectMetadata tierObjectMetadata,
                 Consumer<Boolean> fetchCompletionCallback,
                 long targetOffset,
                 int maxBytes,
                 long maxOffset,
                 List<TopicPartition> ignoredTopicPartitions) {
        this.cancellationContext = cancellationContext;
        this.tierObjectStore = tierObjectStore;
        this.tierObjectMetadata = tierObjectMetadata;
        this.fetchCompletionCallback = fetchCompletionCallback;
        this.targetOffset = targetOffset;
        this.maxBytes = maxBytes;
        this.maxOffset = maxOffset;
        this.ignoredTopicPartitions = ignoredTopicPartitions;
        this.transferPromise = new CompletableFuture<>();
    }

    PendingFetch(CancellationContext cancellationContext,
                 TierObjectStore tierObjectStore,
                 TierObjectMetadata tierObjectMetadata,
                 long targetOffset,
                 int maxBytes,
                 long maxOffset) {
        this(cancellationContext, tierObjectStore, tierObjectMetadata, null, targetOffset,
                maxBytes, maxOffset, Collections.EMPTY_LIST);
    }

    boolean isComplete() {
        return this.transferPromise.isDone();
    }

    /**
     * Complete this fetch by making the provided records available through the transferPromise
     * and calling the fetchCompletionCallback.
     */
    private void completeFetch(MemoryRecords records) {
        this.transferPromise.complete(records);
        if (fetchCompletionCallback != null)
            fetchCompletionCallback.accept(records.sizeInBytes() > 0);
    }

    @Override
    public void run() {
        try {
            final OffsetPosition offsetPosition = OffsetIndexFetchRequest
                    .fetchOffsetPositionForStartingOffset(
                            cancellationContext,
                            tierObjectStore,
                            tierObjectMetadata,
                            targetOffset);
            if (!cancellationContext.isCancelled()) {
                try (final TierObjectStoreResponse response =
                             tierObjectStore.getObject(tierObjectMetadata,
                                     TierObjectStore.TierObjectStoreFileType.SEGMENT,
                                     offsetPosition.position())) {

                    final MemoryRecords records = TierSegmentReader.loadRecords(cancellationContext,
                            response.getInputStream(), maxBytes, maxOffset, targetOffset);
                    bytesFetched = records.sizeInBytes();
                    completeFetch(records);
                }
            } else {
                completeFetch(MemoryRecords.EMPTY);
            }

        } catch (CancellationException e) {
            completeFetch(MemoryRecords.EMPTY);
        } catch (Exception e) {
            this.exception = e;
            completeFetch(MemoryRecords.EMPTY);
        }
    }

    /**
     * Block on a fetch request finishing (or canceling), returning either complete MemoryRecords
     * for the fetch, or empty records.
     */
    public Map<TopicPartition, TierFetchResult> finish() {
        HashMap<TopicPartition, TierFetchResult> resultMap = new HashMap<>();
        try {
            final Records records = transferPromise.get();
            final TierFetchResult tierFetchResult = new TierFetchResult(records, exception);
            resultMap.put(tierObjectMetadata.topicPartition(), tierFetchResult);
        } catch (InterruptedException e) {
            resultMap.put(tierObjectMetadata.topicPartition(), TierFetchResult.emptyFetchResult());
        } catch (ExecutionException e) {
            resultMap.put(tierObjectMetadata.topicPartition(),
                    new TierFetchResult(MemoryRecords.EMPTY, e.getCause()));
        }

        for (TopicPartition ignoredTopicPartition : ignoredTopicPartitions) {
            resultMap.put(ignoredTopicPartition, TierFetchResult.emptyFetchResult()
            );
        }
        return resultMap;
    }

    int bytesFetched() {
        return this.bytesFetched;
    }

    void cancel() {
        this.cancellationContext.cancel();
    }
}
