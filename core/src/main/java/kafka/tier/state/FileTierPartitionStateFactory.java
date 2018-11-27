/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileTierPartitionStateFactory implements TierPartitionStateFactory {
    private final double positionIndexSparsity;
    private final String baseDir;
    private static final String EXTENSION = ".tierlog";

    public FileTierPartitionStateFactory(String baseDir, double positionIndexSparsity) {
        this.baseDir = baseDir;
        this.positionIndexSparsity = positionIndexSparsity;
    }

    public TierPartitionState newTierPartition(TopicPartition topicPartition) {
        return new FileTierPartitionState(baseDir, topicPartition, positionIndexSparsity);
    }

    public Map<TopicPartition, TierPartitionState> scan() throws IOException {
        HashMap<TopicPartition, TierPartitionState> states = new HashMap<>();
        File[] stateFiles = new File(baseDir)
                .listFiles(o -> o.isFile()
                        && o.getName().endsWith(EXTENSION));
        if (stateFiles == null) {
            throw new FileNotFoundException(
                    String.format("Tier status base directory %s does not exist or is not a directory.",
                    baseDir));
        }
        for (File f: stateFiles) {
            final String filename = f.getName();
            final int splitAt = filename.lastIndexOf('_');
            String topic = filename.substring(0, splitAt);
            String partition = filename
                    .substring(splitAt + 1)
                    .replaceAll(EXTENSION, "");
            TopicPartition tp = new TopicPartition(topic, Integer.parseInt(partition));
            states.put(tp, newTierPartition(tp));
        }
        return states;
    }
}
