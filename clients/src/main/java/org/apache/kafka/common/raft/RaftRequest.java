package org.apache.kafka.common.raft;

import org.apache.kafka.common.protocol.ApiMessage;

public abstract class RaftRequest implements RaftMessage {
    private final int requestId;
    private final ApiMessage data;
    private final long createdTimeMs;

    public RaftRequest(int requestId, ApiMessage data, long createdTimeMs) {
        this.requestId = requestId;
        this.data = data;
        this.createdTimeMs = createdTimeMs;
    }

    @Override
    public int requestId() {
        return requestId;
    }

    @Override
    public ApiMessage data() {
        return data;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public static class Inbound extends RaftRequest {
        public Inbound(int requestId, ApiMessage data, long createdTimeMs) {
            super(requestId, data, createdTimeMs);
        }
    }

    public static class Outbound extends RaftRequest {
        private final int destinationId;

        public Outbound(int requestId, ApiMessage data, int destinationId, long createdTimeMs) {
            super(requestId, data, createdTimeMs);
            this.destinationId = destinationId;
        }

        public int destinationId() {
            return destinationId;
        }
    }
}
