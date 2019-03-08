/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.domain.TierObjectMetadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MockInMemoryTierObjectStore implements TierObjectStore, AutoCloseable {
    private final TierObjectStoreConfig config;
    private final ConcurrentHashMap<String, byte[]> keyToBlob;

    public MockInMemoryTierObjectStore(TierObjectStoreConfig config) {
        this.config = config;
        this.keyToBlob = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<String, byte[]> getStored() {
        return keyToBlob;
    }

    @Override
    public TierObjectStoreResponse getObject(
            TierObjectMetadata objectMetadata, TierObjectStoreFileType objectFileType,
            Integer byteOffset, Integer byteOffsetEnd)
            throws IOException {
        String key = keyPath(objectMetadata, objectFileType);
        byte[] blob = keyToBlob.get(key);
        if (blob == null)
            throw new IOException(String.format("No bytes for key %s", key));
        int start = byteOffset == null ? 0 : byteOffset;
        int end = byteOffsetEnd == null ? blob.length : byteOffsetEnd;
        int byteBufferSize = Math.min(end - start, blob.length);
        ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
        buf.put(blob, start, byteBufferSize);
        buf.flip();

        return new MockInMemoryTierObjectStoreResponse(new ByteArrayInputStream(blob), byteBufferSize);
    }

    @Override
    public void close() {
    }

    @Override
    public TierObjectMetadata putSegment(
            TierObjectMetadata objectMetadata, File segmentData,
            File offsetIndexData, File timestampIndexData,
            File producerStateSnapshotData, File transactionIndexData,
            Optional<File> epochState) throws IOException {
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.SEGMENT),
                segmentData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.OFFSET_INDEX),
                offsetIndexData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.TIMESTAMP_INDEX),
                timestampIndexData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.PRODUCER_STATE),
                producerStateSnapshotData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.TRANSACTION_INDEX),
                transactionIndexData);
        if (epochState.isPresent())
            this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.EPOCH_STATE),
                    epochState.get());
        return objectMetadata;
    }

    private String keyPath(TierObjectMetadata objectMetadata, TierObjectStoreFileType fileType) {
        return String.format("%s/topic=%s/partition=%d/%s/%020d_%d.%s",
                config.s3bucket,
                objectMetadata.topicPartition().topic(),
                objectMetadata.topicPartition().partition(),
                objectMetadata.messageId(),
                objectMetadata.startOffset(),
                objectMetadata.tierEpoch(),
                fileType.getSuffix());
    }

    private void writeFileToArray(String filePath, File file) throws IOException {
        try (FileChannel sourceChan = FileChannel.open(file.toPath())) {
            ByteBuffer buf = ByteBuffer.allocate((int) sourceChan.size());
            sourceChan.read(buf);
            keyToBlob.put(filePath, buf.array());
        }
    }

    private static class MockInMemoryTierObjectStoreResponse implements TierObjectStoreResponse {
        private final InputStream inputStream;
        private final long objectSize;

        MockInMemoryTierObjectStoreResponse(InputStream inputStream, long objectSize) {
            this.inputStream = inputStream;
            this.objectSize = objectSize;
        }

        @Override
        public InputStream getInputStream() {
            return this.inputStream;
        }

        @Override
        public Long getObjectSize() {
            return this.objectSize;
        }

        @Override
        public void close() {
            try {
                inputStream.close();
            } catch (IOException ignored) { }
        }
    }

}
