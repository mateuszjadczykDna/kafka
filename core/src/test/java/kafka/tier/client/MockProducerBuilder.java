/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import org.apache.kafka.clients.producer.MockProducer;

public class MockProducerBuilder implements TierTopicProducerBuilder {
    private final MockProducer<byte[], byte[]> producer;

    public MockProducerBuilder() {
        this.producer = new MockProducer<>();

    }

    public MockProducer<byte[], byte[]> setupProducer() {
        return producer;
    }

    public MockProducer<byte[], byte[]> producer() {
        return producer;
    }
}
