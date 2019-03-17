package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.EndEpochRequestData;
import org.apache.kafka.common.message.EndEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class EndEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<EndEpochRequest> {
        private final EndEpochRequestData data;

        public Builder(EndEpochRequestData data) {
            super(ApiKeys.END_EPOCH);
            this.data = data;
        }

        @Override
        public EndEpochRequest build(short version) {
            return new EndEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final EndEpochRequestData data;

    private EndEpochRequest(EndEpochRequestData data, short version) {
        super(ApiKeys.END_EPOCH, version);
        this.data = data;
    }

    public EndEpochRequest(Struct struct, short version) {
        super(ApiKeys.END_EPOCH, version);
        this.data = new EndEpochRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public EndEpochResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        EndEpochResponseData data = new EndEpochResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new EndEpochResponse(data);
    }

}
