/*
 Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.TierListOffsetResponseData;
import org.apache.kafka.common.message.TierListOffsetResponseData.TierListOffsetTopicResponse;
import org.apache.kafka.common.message.TierListOffsetResponseData.TierListOffsetPartitionResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TierListOffsetResponse extends AbstractResponse {
    private final TierListOffsetResponseData data;

    public static final long UNKNOWN_OFFSET = -1L;
    public static final int UNKNOWN_LEADER_EPOCH = -1;

    public TierListOffsetResponse(TierListOffsetResponseData data) {
        this.data = data;
    }

    public TierListOffsetResponse(Struct struct, short version) {
        this.data = new TierListOffsetResponseData(struct, version);
    }

    public TierListOffsetResponseData data() {
        return data;
    }

    public static TierListOffsetResponse parse(ByteBuffer buffer, short version) {
        return new TierListOffsetResponse(ApiKeys.TIER_LIST_OFFSET.responseSchema(version).read(buffer), version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (TierListOffsetTopicResponse topicResponse : data.topics()) {
            for (TierListOffsetPartitionResponse partitionResponse : topicResponse.partitions()) {
                Errors error = Errors.forCode(partitionResponse.errorCode());
                errorCounts.put(error, errorCounts.getOrDefault(error, 0) + 1);
            }
        }
        return errorCounts;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }
}
