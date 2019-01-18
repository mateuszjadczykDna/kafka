/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.io.File;

public class MemoryTierPartitionStateFactory implements TierPartitionStateFactory {
    @Override
    public TierPartitionState initState(File dir, TopicPartition topicPartition, boolean tieringEnabled) {
        return new MemoryTierPartitionState(dir, topicPartition, tieringEnabled);
    }
}
