/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

public class TierObjectStoreConfig {
    public String s3bucket;
    public String s3Region;
    public String s3AwsSecretAccessKey;
    public String s3AwsAccessKeyId;
    public String s3EndpointOverride;
    public String s3SignerOverride;

    public TierObjectStoreConfig(KafkaConfig config) {
        this.s3bucket = config.tierS3Bucket();
        this.s3Region = config.tierS3Region();
        this.s3AwsSecretAccessKey = config.tierS3AwsSecretAccessKey();
        this.s3AwsAccessKeyId = config.tierS3AwsAccessKeyId();
        this.s3EndpointOverride = config.tierS3EndpointOverride();
        this.s3SignerOverride = config.tierS3SignerOverride();
    }

    public TierObjectStoreConfig() { }
}
