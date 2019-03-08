/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.server.DelayedOperation;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TierFetcherTest {
    private MockTime mockTime = new MockTime();

    @Test
    public void tierFetcherExceptionCausesOnComplete() throws Exception {
        ByteBuffer offsetIndexBuffer = ByteBuffer.allocate(1);
        ByteBuffer segmentFileBuffer = ByteBuffer.allocate(1);

        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        TierObjectMetadata tierObjectMetadata = new TierObjectMetadata(topicPartition, 0, 0,
                0, 0, 0, 0, false, false, kafka.tier.serdes.State.AVAILABLE);
        Metrics metrics = new Metrics();
        TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
        try {
            int maxBytes = 600;
            UUID requestId = UUID.randomUUID();

            TierFetchMetadata fetchMetadata = new TierFetchMetadata(0, Option.apply(1000L),
                    maxBytes, 1000L, true, tierObjectMetadata, Option.empty(), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            DelayedOperation delayedFetch = new MockDelayedFetch(f, tierFetcher, requestId);

            tierObjectStore.failNextRequest();
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.inFlightValueMetricName).metricValue(), 0.0);
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
            tierFetcher.fetch(requestId,
                    new ArrayList<>(Arrays.asList(fetchMetadata)),
                    ignored -> delayedFetch.tryComplete());
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.inFlightValueMetricName).metricValue(), 1.0);

            assertTrue(f.get(2000, TimeUnit.MILLISECONDS));
            assertTrue(tierFetcher.bytesFetched(requestId).isPresent());
            assertEquals(tierFetcher.getFetchResultsAndRemove(requestId).get().get(topicPartition).records.sizeInBytes(), 0);
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.inFlightValueMetricName).metricValue(), 0.0);
            // We fetched no bytes because there was an exception.
            assertEquals((double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0, 0);
        } finally {
            tierFetcher.close();
        }
    }

    private ByteBuffer getMemoryRecordsBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        ByteBuffer buffer2 = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        MemoryRecordsBuilder builder2 = MemoryRecords.builder(buffer2, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        IntStream.range(0, 50).forEach(i -> builder.appendWithOffset(i, 1L, "a".getBytes(), "v".getBytes()));
        IntStream.range(50, 101).forEach(i -> builder2.appendWithOffset(i, 1L, "a".getBytes(), "v".getBytes()));
        builder.build();
        builder2.build();
        buffer.flip();
        buffer2.flip();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(2048 * 2048).put(buffer).put(buffer2);
        combinedBuffer.flip();
        return combinedBuffer;
    }

    class MockDelayedFetch extends DelayedOperation {
        private final CompletableFuture<Boolean> tryCompleteFut;
        TierFetcher tierFetcher;
        UUID requestId;
        MockDelayedFetch(CompletableFuture<Boolean> tryCompleteFut, TierFetcher tierFetcher, UUID requestId) {
            super(0, Option.empty());
            this.tryCompleteFut = tryCompleteFut;
            this.tierFetcher = tierFetcher;
            this.requestId = requestId;
        }

        @Override
        public void onExpiration() {

        }

        @Override
        public void onComplete() {
            tryCompleteFut.complete(true);
        }

        @Override
        public boolean tryComplete() {
            if (tierFetcher.isComplete(requestId).orElse(false)) {
                return this.forceComplete();
            } else {
                return false;
            }
        }
    }

    @Test
    public void tierFetcherRequestEmptyIndexTest() throws Exception {
        ByteBuffer combinedBuffer = getMemoryRecordsBuffer();
        TierObjectStore tierObjectStore = new MockedTierObjectStore(combinedBuffer, ByteBuffer.allocate(0));
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        TierObjectMetadata tierObjectMetadata = new TierObjectMetadata(topicPartition, 0, 0, 101,
                0, 0, 0, false, false,
                kafka.tier.serdes.State.AVAILABLE);

        Metrics metrics = new Metrics();
        TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
        try {
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(0, Option.apply(1000L),
                    10000, 1000L, true, tierObjectMetadata,
                    Option.empty(), 0, 1000);

            UUID requestId = UUID.randomUUID();

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            DelayedOperation delayedFetch = new MockDelayedFetch(f, tierFetcher, requestId);
            tierFetcher.fetch(requestId,
                    new ArrayList<>(Arrays.asList(fetchMetadata)),
                    ignored -> delayedFetch.tryComplete());
            f.get(2000, TimeUnit.MILLISECONDS);
            assertTrue(tierFetcher.bytesFetched(requestId).isPresent());

            Map<TopicPartition, TierFetchResult> fetchResults =
                    tierFetcher.getFetchResultsAndRemove(requestId).get();
            assertNotNull("expected non-null fetch result", fetchResults);
            TierFetchResult fetchResult = fetchResults.get(topicPartition);
            Records records = fetchResult.records;

            long lastOffset = 0; // Start looking at offset 0
            for (Record record : records.records()) {
                assertEquals("Offset not expected", record.offset(), lastOffset);
                lastOffset += 1;
            }
        } finally {
            tierFetcher.close();
        }
    }

    private MemoryRecords buildWithOffset(long baseOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        IntStream.range(0, 50).forEach(i -> builder.appendWithOffset(baseOffset + i, 1L, "a".getBytes(), "v".getBytes()));
        return builder.build();
    }

    @Test
    public void tierFetcherIndexTest() throws Exception {
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime, false, 4096, false, "");
        try {
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            int nextOffset = (int) logSegment.readNextOffset();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            TierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer);
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            TierObjectMetadata tierObjectMetadata = new TierObjectMetadata(topicPartition,
                    0, 0, nextOffset, 0, 0,
                    0, false, false, kafka.tier.serdes.State.AVAILABLE);
            Metrics metrics = new Metrics();

            TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
            try {
                TierFetchMetadata fetchMetadata = new TierFetchMetadata(100,
                        Option.apply(1000L), 10000, 1000L, true,
                        tierObjectMetadata, Option.empty(), 0, 1000);
                UUID requestId = UUID.randomUUID();

                CompletableFuture<Boolean> f = new CompletableFuture<>();
                DelayedOperation delayedFetch = new MockDelayedFetch(f, tierFetcher, requestId);
                tierFetcher.fetch(requestId,
                        new ArrayList<>(Arrays.asList(fetchMetadata)),
                        ignored -> delayedFetch.tryComplete());
                f.get(2000, TimeUnit.MILLISECONDS);
                assertTrue(tierFetcher.bytesFetched(requestId).isPresent());

                Map<TopicPartition, TierFetchResult> fetchResults =
                        tierFetcher.getFetchResultsAndRemove(requestId).get();
                assertNotNull("expected non-null fetch result", fetchResults);
                TierFetchResult fetchResult = fetchResults.get(topicPartition);
                Records records = fetchResult.records;

                long lastOffset = 100L; // Start looking at offset 100
                for (Record record : records.records()) {
                    assertEquals("Offset not expected", lastOffset, record.offset());
                    lastOffset += 1;
                }
            } finally {
                tierFetcher.close();
            }

        } finally {
            logSegment.close();
        }
    }

    class MockedTierObjectStore implements TierObjectStore {
        private final ByteBuffer segmentByteBuffer;
        private final ByteBuffer indexByteBuffer;
        private final AtomicBoolean failNextRequest = new AtomicBoolean(false);

        MockedTierObjectStore(ByteBuffer segmentByteBuffer,
                              ByteBuffer indexByteBuffer) {
            this.segmentByteBuffer = segmentByteBuffer;
            this.indexByteBuffer = indexByteBuffer;
        }

        class MockTierObjectStoreResponse implements TierObjectStoreResponse {
            private final InputStream is;
            private final long size;

            MockTierObjectStoreResponse(InputStream is, long size) {
                this.is = is;
                this.size = size;
            }

            @Override
            public InputStream getInputStream() {
                return is;
            }

            @Override
            public Long getObjectSize() {
                return size;
            }

            @Override
            public void close() {
            }
        }

        void failNextRequest() {
            failNextRequest.set(true);
        }

        @Override
        public void close() {
        }

        @Override
        public TierObjectStoreResponse getObject(TierObjectMetadata tierObjectMetadata,
                                                 TierObjectStoreFileType fileType,
                                                 Integer byteOffset, Integer byteOffsetEnd) throws IOException {
            if (failNextRequest.compareAndSet(true, false)) {
                throw new IOException("Failed to retrieve object.");
            }
            if (fileType == TierObjectStoreFileType.OFFSET_INDEX) {
                int start = byteOffset == null ? 0 : byteOffset;
                int end = byteOffsetEnd == null ? indexByteBuffer.array().length : byteOffsetEnd;
                int byteBufferSize = Math.min(end - start, indexByteBuffer.array().length);
                end = Math.min(byteBufferSize, end);
                ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
                buf.put(indexByteBuffer.array(), start, end - start);
                buf.flip();
                return new MockTierObjectStoreResponse(new ByteBufferInputStream(buf), byteBufferSize);

            } else if (fileType == TierObjectStoreFileType.SEGMENT) {
                int start = byteOffset == null ? 0 : byteOffset;
                int end = byteOffsetEnd == null ? segmentByteBuffer.array().length : byteOffsetEnd;
                int byteBufferSize = Math.min(end - start, segmentByteBuffer.array().length);
                ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
                buf.put(segmentByteBuffer.array(), start, end - start);
                buf.flip();
                return new MockTierObjectStoreResponse(new ByteBufferInputStream(buf), byteBufferSize);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public TierObjectMetadata putSegment(TierObjectMetadata objectMetadata, File segmentData,
                                             File offsetIndexData, File timestampIndexData,
                                             File producerStateSnapshotData,
                                             File transactionIndexData, Optional<File> epochState) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void tierFetcherMaxBytesTest() throws Exception {
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime,
                false, 4096, false, "");
        try {
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            int nextOffset = (int) logSegment.readNextOffset();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            TierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer);
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            TierObjectMetadata tierObjectMetadata = new TierObjectMetadata(topicPartition, 0, 0,
                    nextOffset, 0, 0, 0, false, false, kafka.tier.serdes.State.AVAILABLE);
            Metrics metrics = new Metrics();
            TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
            try {
                int maxBytes = 600;
                TierFetchMetadata fetchMetadata = new TierFetchMetadata(0,
                        Option.apply(1000L), maxBytes, 1000L, true,
                        tierObjectMetadata, Option.empty(), 0, 1000);

                UUID requestId = UUID.randomUUID();

                CompletableFuture<Boolean> f = new CompletableFuture<>();
                DelayedOperation delayedFetch = new MockDelayedFetch(f, tierFetcher, requestId);
                assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
                tierFetcher.fetch(requestId,
                        new ArrayList<>(Arrays.asList(fetchMetadata)),
                        ignored -> delayedFetch.tryComplete());
                f.get(5000, TimeUnit.MILLISECONDS);
                assertTrue(tierFetcher.bytesFetched(requestId).isPresent());

                Map<TopicPartition, TierFetchResult> fetchResults =
                        tierFetcher.getFetchResultsAndRemove(requestId).get();
                assertTrue((double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                assertNotNull("expected non-null fetch result", fetchResults);
                TierFetchResult fetchResult = fetchResults.get(topicPartition);
                Records records = fetchResult.records;
                assertTrue(fetchResult.records.sizeInBytes() <= maxBytes);

                long lastOffset = 0L; // Start looking at offset 0
                for (Record record : records.records()) {
                    assertEquals("Offset not expected", record.offset(), lastOffset);
                    lastOffset += 1;
                }
                assertEquals("When we set maxBytes low, we just read the first 50 records "
                        + "successfully.", 50, lastOffset);

            } finally {
                tierFetcher.close();
            }
        } finally {
            logSegment.close();
        }
    }
}