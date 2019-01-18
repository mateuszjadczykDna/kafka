/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;

public interface TierPartitionStateFactory {
    TierPartitionState initState(File dir, TopicPartition topicPartition, boolean tieringEnabled) throws IOException;
}
