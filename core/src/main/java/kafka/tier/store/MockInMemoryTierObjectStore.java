/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.domain.TierObjectMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

public class MockInMemoryTierObjectStore implements TierObjectStore, AutoCloseable {
    private final String bucketName;
    private final ConcurrentHashMap<String, byte[]> keyToBlob;

    public MockInMemoryTierObjectStore(String bucketName) {
        this.bucketName = bucketName;
        this.keyToBlob = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public TierObjectStoreResponse getObject(
            TierObjectMetadata objectMetadata, TierObjectStoreFileType objectFileType,
            Integer byteOffset, Integer byteOffsetEnd)
            throws IOException {
        String key = keyPath(objectMetadata, objectFileType);
        byte[] blob = keyToBlob.get(key);
        if (blob == null) {
            throw new IOException(String.format("No bytes for key %s", key));
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(blob);
        if (byteOffset != null) {
            bis.skip(byteOffset);
        }
        return new MockInMemoryTierObjectStoreResponse(bis, blob.length);
    }

    @Override
    public TierObjectStoreResponse getObjectWithSignedUrl(
            String signedUrl, TierObjectStoreFileType objectFileType,
            Integer byteOffset, Integer byteOffsetEnd) throws IOException {
        String key = signedUrl;
        byte[] blob = keyToBlob.get(key);
        if (blob == null) {
            throw new IOException(String.format("No bytes for key %s", key));
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(blob);
        if (byteOffset != null) {
            bis.skip(byteOffset);
        }
        return new MockInMemoryTierObjectStoreResponse(bis, blob.length);
    }

    @Override
    public String getSignedUrl(TierObjectMetadata objectMetadata,
                               TierObjectStoreFileType objectFileType) {
        return keyPath(objectMetadata, objectFileType);
    }

    @Override
    public TierObjectMetadata putSegment(
            TierObjectMetadata objectMetadata, FileChannel segmentData,
            FileChannel offsetIndexData, FileChannel timestampIndexData,
            FileChannel producerStateSnapshotData, FileChannel transactionIndexData,
            FileChannel epochState) throws IOException {
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
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.EPOCH_STATE),
                epochState);
        return objectMetadata;
    }

    private String keyPath(TierObjectMetadata objectMetadata, TierObjectStoreFileType fileType) {
        return String.format("%s/topic=%s/partition=%d/%d/%d+%d_%d.%s",
                bucketName,
                objectMetadata.topicPartition().topic(),
                objectMetadata.topicPartition().partition(),
                objectMetadata.lastModifiedTime(),
                objectMetadata.startOffset(),
                objectMetadata.endOffsetDelta(),
                objectMetadata.tierEpoch(),
                fileType.getSuffix());
    }

    private void writeFileToArray(String filePath, FileChannel fileChannel) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate((int) fileChannel.size());
        fileChannel.read(buf);
        keyToBlob.put(filePath, buf.array());
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
        public void close() throws Exception {
            inputStream.close();
        }
    }

}
