/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import org.apache.kafka.clients.producer.Producer;

public interface TierTopicProducerBuilder {
    Producer<byte[], byte[]> setupProducer(String bootstrapServers);
}
