/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;

public class TierTopicPartitioner {
    private int numPartitions;

    public TierTopicPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Determine the Tier Topic partitionId that should contain metadata for a given tiered
     * TopicPartition
     * @param topicPartition tiered topic partition
     * @return partitionId
     */
    public int partitionId(TopicPartition topicPartition) {
        byte[] bytes = String.format("%s_%s", topicPartition.topic(), topicPartition.partition())
                .getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(bytes)) % numPartitions;
    }
}
