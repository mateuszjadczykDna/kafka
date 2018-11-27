/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import kafka.tier.TierTopicManagerCommitter;
import org.apache.kafka.clients.consumer.Consumer;

public interface TierTopicConsumerBuilder {
    Consumer<byte[], byte[]> setupConsumer(TierTopicManagerCommitter committer,
                                           String topicName,
                                           String clientIdSuffix);
}
