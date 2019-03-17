package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.FindLeaderRequestData;
import org.apache.kafka.common.message.FindLeaderResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class FindLeaderRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<FindLeaderRequest> {
        private final FindLeaderRequestData data;

        public Builder(FindLeaderRequestData data) {
            super(ApiKeys.FIND_LEADER);
            this.data = data;
        }

        @Override
        public FindLeaderRequest build(short version) {
            return new FindLeaderRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final FindLeaderRequestData data;

    private FindLeaderRequest(FindLeaderRequestData data, short version) {
        super(ApiKeys.FIND_LEADER, version);
        this.data = data;
    }

    public FindLeaderRequest(Struct struct, short version) {
        super(ApiKeys.FIND_LEADER, version);
        this.data = new FindLeaderRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public FindLeaderResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FindLeaderResponseData data = new FindLeaderResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new FindLeaderResponse(data);
    }

}
