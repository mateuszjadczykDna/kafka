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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class AddPartitionsToTxnResponse extends AbstractResponse {
    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   InvalidTxnState
    //   InvalidProducerIdMapping
    //   TopicAuthorizationFailed
    //   InvalidProducerEpoch
    //   UnknownTopicOrPartition
    //   TopicAuthorizationFailed
    //   TransactionalIdAuthorizationFailed
    public final AddPartitionsToTxnResponseData data;

    public AddPartitionsToTxnResponse(int throttleTimeMs, Map<TopicPartition, Errors> errors) {
        Map<String, AddPartitionsToTxnPartitionResult> offsetFetchResponseTopicMap = new HashMap<>();
        AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection
        for (Map.Entry<TopicPartition, PartitionData> entry : responseData.entrySet()) {
            String topicName = entry.getKey().topic();
            OffsetFetchResponseTopic topic = offsetFetchResponseTopicMap.getOrDefault(
                topicName, new OffsetFetchResponseTopic().setName(topicName));
            PartitionData partitionData = entry.getValue();
            topic.partitions().add(new OffsetFetchResponsePartition()
                                       .setPartitionIndex(entry.getKey().partition())
                                       .setErrorCode(partitionData.error.code())
                                       .setCommittedOffset(partitionData.offset)
                                       .setCommittedLeaderEpoch(partitionData.leaderEpoch.orElse(0))
                                       .setMetadata(partitionData.metadata)
            );
            offsetFetchResponseTopicMap.put(topicName, topic);
        }
    }

    public AddPartitionsToTxnResponse(Struct struct, short version) {
        this.data = new AddPartitionsToTxnResponseData(struct, version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public Map<TopicPartition, Errors> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(errors);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static AddPartitionsToTxnResponse parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnResponse(ApiKeys.ADD_PARTITIONS_TO_TXN.parseResponse(version, buffer), version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
