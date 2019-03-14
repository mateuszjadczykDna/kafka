package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.AppendRecordsRequestData;
import org.apache.kafka.common.message.AppendRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class AppendRecordsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<AppendRecordsRequest> {
        private final AppendRecordsRequestData data;

        public Builder(AppendRecordsRequestData data) {
            super(ApiKeys.FETCH_RECORDS);
            this.data = data;
        }

        @Override
        public AppendRecordsRequest build(short version) {
            return new AppendRecordsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final AppendRecordsRequestData data;
    public final short version;

    private AppendRecordsRequest(AppendRecordsRequestData data, short version) {
        super(ApiKeys.FETCH_RECORDS, version);
        this.data = data;
        this.version = version;
    }

    public AppendRecordsRequest(Struct struct, short version) {
        super(ApiKeys.FETCH_RECORDS, version);
        this.data = new AppendRecordsRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AppendRecordsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        AppendRecordsResponseData data = new AppendRecordsResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new AppendRecordsResponse(data);
    }

}
