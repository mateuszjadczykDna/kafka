package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class VoteRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<VoteRequest> {
        private final VoteRequestData data;

        public Builder(VoteRequestData data) {
            super(ApiKeys.VOTE);
            this.data = data;
        }

        @Override
        public VoteRequest build(short version) {
            return new VoteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final VoteRequestData data;
    public final short version;

    private VoteRequest(VoteRequestData data, short version) {
        super(ApiKeys.VOTE, version);
        this.data = data;
        this.version = version;
    }

    public VoteRequest(Struct struct, short version) {
        super(ApiKeys.VOTE, version);
        this.data = new VoteRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public VoteResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        VoteResponseData data = new VoteResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new VoteResponse(data);
    }

}
