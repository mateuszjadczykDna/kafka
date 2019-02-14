// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.message.RequestHeaderData;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a tenant connection and any associated resources, such as a broker
 * connection.
 */
public final class Flow implements AutoCloseable {
    /**
     * The next flow ID to use.
     *
     * We initialize this based on the current time, to make it more
     * likely that different runs of Aegis will have different flow IDs.
     * This helps when analyzing log files.
     */
    private static final AtomicLong NEXT_FLOW_ID = new AtomicLong(new Date().getTime());

    /**
     * The worker pool that manages this flow.
     */
    private final WorkerPool pool;

    /**
     * The netty channel used to communicate with the tenant.
     */
    private final Channel tenantChannel;

    /**
     * The unique ID of this flow.
     */
    private final long id;

    /**
     * The slf4j logger for general flow logging.
     */
    private final Logger log;

    /**
     * The connection to the broker, or null if there is none.
     */
    private Channel brokerChannel;

    /**
     * The pending connection to the broker, or null if there is none.
     */
    private PendingBrokerConnection pendingBrokerConnection;

    /**
     * Requests to the broker that have not yet received a response.
     */
    private Map<Integer, RequestHeaderData> pendingBrokerRequests;

    private class PendingBrokerConnection implements ChannelFutureListener {
        private final InetSocketAddress brokerAddress;
        private final ArrayList<Object> pendingEvents;

        PendingBrokerConnection(InetSocketAddress brokerAddress) {
            this.pendingEvents = new ArrayList<>();
            this.brokerAddress = brokerAddress;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                log.info("Failed to connect to broker at {}", brokerAddress, future.cause());
                close();
                return;
            }
            Channel ch = future.channel();
            if (log.isDebugEnabled()) {
                log.debug("Connected to broker at {}", ch.remoteAddress());
            }
            synchronized (Flow.this) {
                Flow.this.brokerChannel = ch;
                Flow.this.pendingBrokerConnection = null;
                for (Object in : pendingEvents) {
                    sendToConnectedBroker(in);
                }
            }
        }
    }

    Flow(WorkerPool pool, Channel tenantChannel) throws Exception {
        this.pool = pool;
        this.tenantChannel = tenantChannel;
        this.id = NEXT_FLOW_ID.addAndGet(1);
        this.log = new LogContext(String.
            format("Flow-%d-%d: ", pool.brokerIndex(), id)).logger(Flow.class);
        this.brokerChannel = null;
        this.pendingBrokerConnection = null;
        this.pendingBrokerRequests = new HashMap<>();
    }

    long id() {
        return id;
    }

    int brokerIndex() {
        return pool.brokerIndex();
    }

    Logger log() {
        return log;
    }

    @Override
    public void close() {
        pool.remove(this);
        synchronized (this) {
            tenantChannel.close();
            if (brokerChannel != null) {
                brokerChannel.close();
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = (int) (id & 0xffffffff);
        hash ^= (int) ((id >> 32) & 0xffffffff);
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Flow)) {
            return false;
        }
        Flow other = (Flow) o;
        return other.id == this.id;
    }

    /**
     * Send a message to the broker associated with this flow.
     */
    synchronized void sendToBroker(Object in) throws Exception {
        if (brokerChannel != null) {
            if (log.isTraceEnabled()) {
                log.trace("Sending {} to connected broker.", in);
            }
            sendToConnectedBroker(in);
        } else {
            if (pendingBrokerConnection == null) {
                InetSocketAddress brokerAddress =
                    pool.endPointPair().brokerEndpoint().resolvedAddress();
                if (log.isTraceEnabled()) {
                    log.trace("Connecting to broker {} at {}", pool.brokerIndex(), brokerAddress);
                }
                this.pendingBrokerConnection = new PendingBrokerConnection(brokerAddress);
                pool.brokerBootstrap().clone().
                    handler(new WorkerPool.BrokerChannelInitializer(this)).
                    connect(brokerAddress).
                    addListener(pendingBrokerConnection);
            }
            if (log.isTraceEnabled()) {
                log.trace("Queuing {} for broker.", in);
            }
            pendingBrokerConnection.pendingEvents.add(in);
        }
    }

    private synchronized void sendToConnectedBroker(Object in) {
        if (in instanceof FrameStart) {
            @SuppressWarnings("unchecked")
            FrameStart frameStart = (FrameStart) in;
            pendingBrokerRequests.put(frameStart.header().correlationId(), frameStart.header());
        }
        brokerChannel.write(in);
    }

    synchronized RequestHeaderData findAndRemovePending(int correlationId) {
        return pendingBrokerRequests.remove(correlationId);
    }

    synchronized void sendToTenant(Object in) {
        if (log.isTraceEnabled()) {
            log.trace("Sending {} to tenant.", in);
        }
        tenantChannel.write(in);
    }
};
