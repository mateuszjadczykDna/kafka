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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.util.concurrent.CompletableFuture;

public class NoOpStateMachine implements ReplicatedStateMachine {
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);

    private RecordAppender recordAppender = null;

    private RaftState nodeState = RaftState.UNINITIALIZED;

    private int epoch = -1;

    boolean isLeader() {
        return nodeState == RaftState.LEADER;
    }

    boolean isFollower() {
        return nodeState == RaftState.NON_LEADER;
    }

    @Override
    public void initialize(RecordAppender recordAppender) {
        this.recordAppender = recordAppender;
        nodeState = RaftState.NON_LEADER;
    }

    @Override
    public void becomeLeader(int epoch) {
        this.epoch = epoch;
        nodeState = RaftState.LEADER;
    }

    @Override
    public void becomeFollower(int epoch) {
        this.epoch = epoch;
        nodeState = RaftState.NON_LEADER;
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    @Override
    public boolean accept(Records records) {
        return true;
    }

    CompletableFuture<OffsetAndEpoch> append(Records records) {
        if (recordAppender == null) {
            throw new IllegalStateException("Record appender is not set");
        }
        if (nodeState != RaftState.LEADER) {
            throw new IllegalStateException("The raft client is not leader yet");
        }
        return recordAppender.append(records);
    }

    @Override
    public void close() {
        clear();
    }

    void clear() {
        nodeState = RaftState.NON_LEADER;
        epoch = -1;
    }

    public int epoch() {
        return epoch;
    }
}
