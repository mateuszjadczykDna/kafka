/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.domain.TierObjectMetadata;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface TierObjectStore {
    enum TierObjectStoreFileType {

        SEGMENT("segment"),
        OFFSET_INDEX("offset-index"),
        TIMESTAMP_INDEX("timestamp-index"),
        TRANSACTION_INDEX("transaction-index"),
        PRODUCER_STATE("producer-status"),
        EPOCH_STATE("epoch_state");

        private final String suffix;

        public String getSuffix() {
            return suffix;
        }

        TierObjectStoreFileType(String suffix) {
            this.suffix = suffix;
        }
    }

    TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata,
                                      TierObjectStoreFileType objectFileType,
                                      Integer byteOffset, Integer byteOffsetEnd)
            throws IOException;

    TierObjectStoreResponse getObjectWithSignedUrl(
            String signedUrl,
            TierObjectStoreFileType objectFileType,
            Integer byteOffset, Integer byteOffsetEnd)
            throws IOException;

    String getSignedUrl(TierObjectMetadata objectMetadata,
                        TierObjectStoreFileType objectFileType);

    TierObjectMetadata putSegment(TierObjectMetadata objectMetadata,
                                  FileChannel segmentData,
                                  FileChannel offsetIndexData,
                                  FileChannel timestampIndexData,
                                  FileChannel producerStateSnapshotData,
                                  FileChannel transactionIndexData,
                                  FileChannel epochState)
            throws IOException;
}
