/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class S3TierObjectStore implements TierObjectStore {
    private final static int PART_UPLOAD_SIZE = 5 * 1024 * 1024;
    private final static Logger log = LoggerFactory.getLogger(S3TierObjectStore.class);
    private final String bucket;
    private AmazonS3 client;

    public S3TierObjectStore(TierObjectStoreConfig config) {
        this.client = client(config);
        this.bucket = config.s3bucket;
        ensureBucket(false);
    }

    public void ensureBucket(boolean create) throws TierObjectStoreFatalException {
        try {
            client.getBucketAcl(bucket);
        } catch (final AmazonServiceException aclException) {
            if (aclException.getStatusCode() == 301) {
                // Bucket exists, but redirect status indicates region mismatch
                throw new TierObjectStoreFatalException("S3 bucket region does not match S3 client region", aclException);
            } else if ("AccessDenied".equals(aclException.getErrorCode())) {
                // Bucket exists, but client lacks valid credentials
                throw new TierObjectStoreFatalException(
                    "Invalid credentials for S3 bucket", aclException);
            } else if (aclException.getStatusCode() == 404) {
                // Bucket does not exist
                if (create) {
                    // create bucket if it does not exist
                    try {
                        client.createBucket(bucket);
                    } catch (final AmazonClientException createException) {
                        throw new TierObjectStoreFatalException("Failed to create S3 bucket",
                                createException);
                    }
                } else {
                    throw new TierObjectStoreFatalException(
                            String.format("Bucket %s does not exist", bucket), aclException);
                }
            } else {
                // Other getBucketAcl exception
                throw new TierObjectStoreFatalException("Failed to test S3 bucket access",
                    aclException);
            }
        }
    }

    @Override
    public TierObjectStoreResponse getObject(
            TierObjectMetadata objectMetadata, TierObjectStoreFileType objectFileType,
            Integer byteOffsetStart, Integer byteOffsetEnd)
            throws AmazonServiceException {
        final String key = keyPath(objectMetadata, objectFileType);
        final GetObjectRequest request = new GetObjectRequest(bucket, key);
        if (byteOffsetStart != null && byteOffsetEnd != null)
            request.setRange(byteOffsetStart, byteOffsetEnd);
        else if (byteOffsetStart != null)
            request.setRange(byteOffsetStart);
        else if (byteOffsetEnd != null)
            throw new IllegalStateException("Cannot specify a byteOffsetEnd without specifying a "
                    + "byteOffsetStart");
        log.debug("Fetching object from s3://{}/{}, with range start {}", bucket, key, byteOffsetStart);
        final S3Object object = client.getObject(request);
        final S3ObjectInputStream inputStream = object.getObjectContent();
        return new S3TierObjectStoreResponse(inputStream, object.getObjectMetadata().getContentLength());
    }

    @Override
    public void close() {
        this.client.shutdown();
    }

    @Override
    public TierObjectMetadata putSegment(
            TierObjectMetadata objectMetadata, File segmentData,
            File offsetIndexData, File timestampIndexData,
            File producerStateSnapshotData, File transactionIndexData,
            Optional<File> epochState) {
        try {
            putFileMultipart(keyPath(objectMetadata, TierObjectStoreFileType.SEGMENT), segmentData);
            putFile(keyPath(objectMetadata, TierObjectStoreFileType.OFFSET_INDEX), offsetIndexData);
            putFile(keyPath(objectMetadata, TierObjectStoreFileType.TIMESTAMP_INDEX),
                timestampIndexData);
            putFile(keyPath(objectMetadata, TierObjectStoreFileType.PRODUCER_STATE),
                producerStateSnapshotData);
            putFile(keyPath(objectMetadata, TierObjectStoreFileType.TRANSACTION_INDEX),
                transactionIndexData);
            epochState.ifPresent(file -> putFile(keyPath(objectMetadata, TierObjectStoreFileType.EPOCH_STATE), file));

            return objectMetadata;
        } catch (final AmazonClientException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segment objects to S3", e);
        }
    }

    private String keyPath(TierObjectMetadata objectMetadata, TierObjectStoreFileType fileType) {
        return String.format("topic=%s/partition=%d/%s/%020d_%d.%s",
                objectMetadata.topicPartition().topic(),
                objectMetadata.topicPartition().partition(),
                objectMetadata.messageId(),
                objectMetadata.startOffset(),
                objectMetadata.tierEpoch(),
                fileType.getSuffix());
    }

    private void putFile(String key, File file) {
        final PutObjectRequest request = new PutObjectRequest(bucket, key, file);
        log.debug("Uploading object to s3://{}/{}", bucket, key);
        client.putObject(request);
    }

    private void putFileMultipart(String key, File file) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        long fileLength = file.length();
        long partSize = PART_UPLOAD_SIZE;
        log.debug("Uploading multipart object to s3://{}/{}", bucket, key);

        final List<PartETag> partETags = new ArrayList<>();
        final InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucket, key, objectMetadata);
        final InitiateMultipartUploadResult initiateMultipartUploadResult =
                client.initiateMultipartUpload(initiateMultipartUploadRequest);

        long filePosition = 0;
        for (int partNum = 1; filePosition < fileLength; partNum++) {
            partSize = Math.min(partSize, fileLength - filePosition);
            UploadPartRequest uploadPartRequest = new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(initiateMultipartUploadResult.getUploadId())
                    .withPartNumber(partNum)
                    .withFile(file)
                    .withFileOffset(filePosition)
                    .withPartSize(partSize);

            UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
            partETags.add(uploadPartResult.getPartETag());
            filePosition += partSize;
        }

        final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(
                        bucket, key, initiateMultipartUploadResult.getUploadId(), partETags);
        client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private AmazonS3 client(TierObjectStoreConfig config) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setClientConfiguration(clientConfiguration);

        if (config.s3SignerOverride != null && !config.s3SignerOverride.isEmpty())
            clientConfiguration.setSignerOverride(config.s3SignerOverride);

        if (config.s3EndpointOverride != null && !config.s3EndpointOverride.isEmpty()) {
            // If the endpoint is specified, we can set an endpoint and use path style access.
            builder.setEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                            config.s3EndpointOverride,
                            Regions.fromName(config.s3Region).getName()
                    ));
            builder.setPathStyleAccessEnabled(true);

        }

        if (config.s3Region != null)
            builder.setRegion(config.s3Region);

        if (config.s3AwsAccessKeyId != null && config.s3AwsSecretAccessKey != null) {
            final BasicAWSCredentials credentials = new BasicAWSCredentials(config.s3AwsAccessKeyId,
                    config.s3AwsSecretAccessKey);
            builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        } else {
            builder.setCredentials(new DefaultAWSCredentialsProviderChain());
        }

        return builder.build();
    }

    private static class S3TierObjectStoreResponse implements TierObjectStoreResponse {
        private final AutoAbortingS3InputStream inputStream;
        private final long objectSize;

        S3TierObjectStoreResponse(S3ObjectInputStream inputStream, long objectSize) {
            this.inputStream = new AutoAbortingS3InputStream(inputStream, objectSize);
            this.objectSize = objectSize;
        }

        @Override
        public void close() {
            inputStream.close();
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }

        @Override
        public Long getObjectSize() {
            return objectSize;
        }
    }

}
