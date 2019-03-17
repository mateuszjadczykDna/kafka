package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.FetchRecordsRequestData;
import org.apache.kafka.common.message.FetchRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class FetchRecordsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<FetchRecordsRequest> {
        private final FetchRecordsRequestData data;

        public Builder(FetchRecordsRequestData data) {
            super(ApiKeys.FETCH_RECORDS);
            this.data = data;
        }

        @Override
        public FetchRecordsRequest build(short version) {
            return new FetchRecordsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final FetchRecordsRequestData data;

    private FetchRecordsRequest(FetchRecordsRequestData data, short version) {
        super(ApiKeys.FETCH_RECORDS, version);
        this.data = data;
    }

    public FetchRecordsRequest(Struct struct, short version) {
        super(ApiKeys.FETCH_RECORDS, version);
        this.data = new FetchRecordsRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public FetchRecordsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FetchRecordsResponseData data = new FetchRecordsResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new FetchRecordsResponse(data);
    }

}
