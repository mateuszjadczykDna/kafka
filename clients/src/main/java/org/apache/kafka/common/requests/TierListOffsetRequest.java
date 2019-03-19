/*
 Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.TierListOffsetRequestData;
import org.apache.kafka.common.message.TierListOffsetRequestData.TierListOffsetTopic;
import org.apache.kafka.common.message.TierListOffsetRequestData.TierListOffsetPartition;
import org.apache.kafka.common.message.TierListOffsetResponseData;
import org.apache.kafka.common.message.TierListOffsetResponseData.TierListOffsetTopicResponse;
import org.apache.kafka.common.message.TierListOffsetResponseData.TierListOffsetPartitionResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.HashMap;
import java.util.Map;

/**
 * Tiered storage aware `ListOffsetRequest`. With tiered storage, semantics of querying the beginning or end offset
 * using `ListOffsetRequest` remain unchanged - it returns the true log start and end offset respectively, including
 * any tiered portion of the log. `TierListOffsetRequest` is tiering aware and provides a mechanism to query to the
 * local log start offset for example. See {@link OffsetType} for an exhaustive list of functionality this request
 * provides.
 */
public class TierListOffsetRequest extends AbstractRequest {
    private final TierListOffsetRequestData data;
    private final short version;

    private TierListOffsetRequest(TierListOffsetRequestData data, short version) {
        super(ApiKeys.TIER_LIST_OFFSET, version);
        this.data = data;
        this.version = version;
    }

    public TierListOffsetRequest(Struct struct, short version) {
        super(ApiKeys.TIER_LIST_OFFSET, version);
        this.data = new TierListOffsetRequestData(struct, version);
        this.version = version;
    }

    public TierListOffsetRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        TierListOffsetResponseData response = new TierListOffsetResponseData();

        ApiError apiError = ApiError.fromThrowable(e);
        for (TierListOffsetTopic topic : data.topics()) {
            TierListOffsetTopicResponse topicResponse = new TierListOffsetResponseData.TierListOffsetTopicResponse().setName(topic.name());

            for (TierListOffsetPartition partition : topic.partitions()) {
                topicResponse.partitions().add(new TierListOffsetPartitionResponse()
                    .setPartitionIndex(partition.partitionIndex())
                    .setErrorCode(apiError.error().code()));
            }

            response.topics().add(topicResponse);
        }

        return new TierListOffsetResponse(response);
    }

    public enum OffsetType {
        /**
         * Lookup the local log start offset.
         */
        LOCAL_START_OFFSET((byte) 0),
        LOCAL_END_OFFSET((byte) 1);

        private static final Map<Byte, OffsetType> ID_TO_TYPE = new HashMap<>(values().length);
        private byte id;

        static {
            for (OffsetType offsetType : values()) {
                OffsetType oldValue = ID_TO_TYPE.put(offsetType.id, offsetType);
                if (oldValue != null)
                    throw new ExceptionInInitializerError("Duplicate id " + offsetType.id);
            }
        }

        OffsetType(byte id) {
            this.id = id;
        }

        public static OffsetType forId(byte id) {
            if (!hasId(id))
                throw new IllegalArgumentException("Unexpected id " + id);
            return ID_TO_TYPE.get(id);
        }

        public static byte toId(OffsetType offsetType) {
            return offsetType.id;
        }

        private static boolean hasId(byte id) {
            return ID_TO_TYPE.containsKey(id);
        }
    }

    public static class Builder extends AbstractRequest.Builder<TierListOffsetRequest> {
        private final TierListOffsetRequestData data;

        public Builder(TierListOffsetRequestData data) {
            super(ApiKeys.TIER_LIST_OFFSET);
            this.data = data;
        }

        @Override
        public TierListOffsetRequest build(short version) {
            return new TierListOffsetRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
