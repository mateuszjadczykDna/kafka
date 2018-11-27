/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.Map;

public interface TierPartitionStateFactory {
    TierPartitionState newTierPartition(TopicPartition topicPartition);

    /**
     * Scan for existing TierPartitionState states present for this TierPartitionStatus type.
     * For the disk representation, this will load the TierPartitionStates for a given path.
     * This is primarily useful when restarting an existing broker.
     * @return TierPartitionStatus Map keyed by TopicPartition.
     */
    Map<TopicPartition, TierPartitionState> scan() throws IOException;
}
