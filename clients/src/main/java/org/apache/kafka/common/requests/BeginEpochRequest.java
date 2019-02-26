package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.BeginEpochRequestData;
import org.apache.kafka.common.message.BeginEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class BeginEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<BeginEpochRequest> {
        private final BeginEpochRequestData data;

        public Builder(BeginEpochRequestData data) {
            super(ApiKeys.BEGIN_EPOCH);
            this.data = data;
        }

        @Override
        public BeginEpochRequest build(short version) {
            return new BeginEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final BeginEpochRequestData data;
    public final short version;

    private BeginEpochRequest(BeginEpochRequestData data, short version) {
        super(ApiKeys.BEGIN_EPOCH, version);
        this.data = data;
        this.version = version;
    }

    public BeginEpochRequest(Struct struct, short version) {
        super(ApiKeys.BEGIN_EPOCH, version);
        this.data = new BeginEpochRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public BeginEpochResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        BeginEpochResponseData data = new BeginEpochResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new BeginEpochResponse(data);
    }

}
