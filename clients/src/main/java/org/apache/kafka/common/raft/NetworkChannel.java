package org.apache.kafka.common.raft;

import java.util.List;

/**
 * A simple network interface with few assumptions. We do not assume ordering
 * of requests or even that every request will receive a response.
 */
public interface NetworkChannel {

    int newRequestId();

    void send(RaftMessage message);

    List<RaftMessage> receive(long timeoutMs);

    void wakeup();

}
