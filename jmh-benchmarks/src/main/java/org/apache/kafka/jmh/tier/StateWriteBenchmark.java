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

package org.apache.kafka.jmh.tier;

import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.state.FileTierPartitionStateFactory;
import kafka.tier.state.TierPartitionState;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StateWriteBenchmark {
    private static final int COUNT = 10000;
    private static final String BASE_DIR = System.getProperty("java.io.tmpdir");
    private static final int EPOCH = 0;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("mytopic", 0);

    private void writeState(TierPartitionState state) throws IOException {
        state.beginCatchup();
        state.append(new TierTopicInitLeader(TOPIC_PARTITION, EPOCH,
                UUID.randomUUID(), 0));
        for (int i = 0; i < COUNT; i++) {
            state.append(new TierObjectMetadata(TOPIC_PARTITION, EPOCH,
                    i * 2, 1, i, i, i, i, false,  (byte) 0));
        }
        state.flush();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void appendReadByteBufferBench() throws Exception {
        FileTierPartitionStateFactory factory = new FileTierPartitionStateFactory();
        TierPartitionState state = factory.initState(new File(BASE_DIR), TOPIC_PARTITION, true);
        try {
            writeState(state);
        } finally {
            state.delete();
        }
    }
}
