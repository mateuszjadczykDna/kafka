/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleTransactionalWriter {

    private static final int MAX_BLOCK_TIMEOUT = 1000;

    private final int partitionCap;
    private final TransactionManager transactionManager;
    private final RecordAccumulator accumulator;
    private final Map<TopicPartition, PartitionState> resourcePartitions;
    private final Random random;
    private final String outputTopic;
    private final MockTime timer;

    enum PartitionState {
        IN_TXN,
        IDLE,
        ABORTED
    }

    private static class PartitionMetadata {
        PartitionState state;
        int offset;

        PartitionMetadata(PartitionState state) {
            this.state = state;
            this.offset = 0;
        }
    }

    SimpleTransactionalWriter(TransactionManager transactionManager,
                              RecordAccumulator accumulator,
                              final int partitionCap,
                              final Set<TopicPartition> resourcePartitions,
                              final String outputTopic,
                              final MockTime timer) {
        this.partitionCap = partitionCap;
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.resourcePartitions = resourcePartitions.stream().collect(
            Collectors.toMap(partition -> partition, partition -> PartitionState.IDLE));
        this.random = new Random();
        this.outputTopic = outputTopic;
        this.timer = timer;
    }

    void write() throws InterruptedException {
        TopicPartition partitionToWrite = new TopicPartition(outputTopic, random.nextInt(resourcePartitions.size()));
        accumulator.append(partitionToWrite,
            timer.milliseconds(), new byte[1], new byte[1], Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT, false, timer.milliseconds());
        transactionManager.maybeAddPartitionToTransaction(partitionToWrite);
    }

    void beginTransaction() {
        transactionManager.beginTransaction();
    }

    void endTransaction(final boolean abort) {
        if (abort) {
            transactionManager.beginAbort();
        } else {
            transactionManager.beginCommit();
        }
    }

    void onWriteSuccess() {

    }

    void onWriteFail() {

    }
}
