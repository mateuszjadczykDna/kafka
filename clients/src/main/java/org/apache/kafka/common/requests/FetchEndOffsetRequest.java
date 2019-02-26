package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.FetchEndOffsetRequestData;
import org.apache.kafka.common.message.FetchEndOffsetResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class FetchEndOffsetRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<FetchEndOffsetRequest> {
        private final FetchEndOffsetRequestData data;

        public Builder(FetchEndOffsetRequestData data) {
            super(ApiKeys.FETCH_END_OFFSET);
            this.data = data;
        }

        @Override
        public FetchEndOffsetRequest build(short version) {
            return new FetchEndOffsetRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final FetchEndOffsetRequestData data;
    public final short version;

    private FetchEndOffsetRequest(FetchEndOffsetRequestData data, short version) {
        super(ApiKeys.FETCH_END_OFFSET, version);
        this.data = data;
        this.version = version;
    }

    public FetchEndOffsetRequest(Struct struct, short version) {
        super(ApiKeys.FETCH_END_OFFSET, version);
        this.data = new FetchEndOffsetRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public FetchEndOffsetResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FetchEndOffsetResponseData data = new FetchEndOffsetResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new FetchEndOffsetResponse(data);
    }

}
