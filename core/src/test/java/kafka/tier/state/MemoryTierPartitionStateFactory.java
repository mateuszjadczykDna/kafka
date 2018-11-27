/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class MemoryTierPartitionStateFactory implements TierPartitionStateFactory {

    public TierPartitionState newTierPartition(TopicPartition topicPartition) {
        return new MemoryTierPartitionState(topicPartition);
    }

    public Map<TopicPartition, TierPartitionState> scan() {
        return new HashMap<>();
    }
}
