package org.apache.kafka.common.raft;

import org.apache.kafka.common.protocol.ApiMessage;

public abstract class RaftResponse implements RaftMessage {
    protected final int requestId;
    protected final ApiMessage data;

    protected RaftResponse(int requestId, ApiMessage data) {
        this.requestId = requestId;
        this.data = data;
    }

    @Override
    public int requestId() {
        return requestId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public static class Inbound extends RaftResponse {
        private final int sourceId;

        protected Inbound(int requestId, ApiMessage data, int sourceId) {
            super(requestId, data);
            this.sourceId = sourceId;
        }

        public int sourceId() {
            return sourceId;
        }

        @Override
        public String toString() {
            return "Inbound(" +
                    "requestId=" + requestId +
                    ", data=" + data +
                    ", sourceId=" + sourceId +
                    ')';
        }
    }

    public static class Outbound extends RaftResponse {
        protected Outbound(int requestId, ApiMessage data) {
            super(requestId, data);
        }

        @Override
        public String toString() {
            return "OutboundResponse(" +
                    "requestId=" + requestId +
                    ", data=" + data +
                    ')';
        }
    }
}
