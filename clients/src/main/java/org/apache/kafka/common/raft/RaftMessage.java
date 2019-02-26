package org.apache.kafka.common.raft;

import org.apache.kafka.common.protocol.ApiMessage;

public interface RaftMessage {
    int requestId();

    ApiMessage data();

}
