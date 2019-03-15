package org.apache.kafka.common.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNetworkChannel implements NetworkChannel {
    private final AtomicInteger requestIdCounter;
    private List<RaftMessage> sendQueue = new ArrayList<>();
    private List<RaftMessage> receiveQueue = new ArrayList<>();

    public MockNetworkChannel(AtomicInteger requestIdCounter) {
        this.requestIdCounter = requestIdCounter;
    }

    public MockNetworkChannel() {
        this(new AtomicInteger(0));
    }

    @Override
    public int newRequestId() {
        return requestIdCounter.getAndIncrement();
    }

    @Override
    public void send(RaftMessage message) {
        sendQueue.add(message);
    }

    @Override
    public List<RaftMessage> receive(long timeoutMs) {
        List<RaftMessage> messages = receiveQueue;
        receiveQueue = new ArrayList<>();
        return messages;
    }

    @Override
    public void wakeup() {}

    public List<RaftMessage> drainSendQueue() {
        List<RaftMessage> messages = sendQueue;
        sendQueue = new ArrayList<>();
        return messages;
    }

    public void mockReceive(RaftMessage message) {
        receiveQueue.add(message);
    }

    void clear() {
        sendQueue.clear();
        receiveQueue.clear();
        requestIdCounter.set(0);
    }

}
