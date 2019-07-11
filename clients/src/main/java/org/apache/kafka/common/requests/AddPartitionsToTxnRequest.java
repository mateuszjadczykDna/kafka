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
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.OffsetFetchRequestData;
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

import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PRODUCER_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.CommonFields.TRANSACTIONAL_ID;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class AddPartitionsToTxnRequest extends AbstractRequest {

    public final AddPartitionsToTxnRequestData data;

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {

        public final AddPartitionsToTxnRequestData data;

        public Builder(String transactionalId, long producerId, short producerEpoch, List<TopicPartition> partitions) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN);

            Map<String, AddPartitionsToTxnTopic> requestTopicMap = new HashMap<>();
            partitions.forEach(topicPartition -> {
                String topicName = topicPartition.topic();
                AddPartitionsToTxnTopic topic = requestTopicMap.getOrDefault(
                    topicName, new AddPartitionsToTxnTopic().setName(topicName));
                topic.partitions().add(topicPartition.partition());
                requestTopicMap.put(topicName, topic);
            });

            this.data = new AddPartitionsToTxnRequestData()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerId)
                            .setProducerEpoch(producerEpoch)
                            .setTopics(new AddPartitionsToTxnTopicCollection(
                                requestTopicMap.values().iterator()));
        }

        @Override
        public AddPartitionsToTxnRequest build(short version) {
            return new AddPartitionsToTxnRequest(data, version);
        }

        public List<TopicPartition> partitions() {
            return AddPartitionsToTxnRequest.partitions(data);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }


    private AddPartitionsToTxnRequest(AddPartitionsToTxnRequestData data, short version) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN, version);
        this.data = data;
    }

    public AddPartitionsToTxnRequest(Struct struct, short version) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN, version);
        this.data = new AddPartitionsToTxnRequestData(struct, version);
    }

    public List<TopicPartition> partitions() {
        return partitions(data);
    }

    private static List<TopicPartition> partitions(AddPartitionsToTxnRequestData data) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (AddPartitionsToTxnTopic topic : data.topics()) {
            for (Integer partitionIndex : topic.partitions()) {
                partitions.add(new TopicPartition(topic.name(), partitionIndex));
            }
        }
        return partitions;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        final HashMap<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions(data)) {
            errors.put(partition, Errors.forException(e));
        }
        return new AddPartitionsToTxnResponse(throttleTimeMs, errors);
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(ApiKeys.ADD_PARTITIONS_TO_TXN.parseRequest(version, buffer), version);
    }
}
