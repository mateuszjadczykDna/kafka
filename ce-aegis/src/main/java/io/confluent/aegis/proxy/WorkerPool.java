// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import static io.confluent.aegis.config.AegisConfig.NUM_WORKER_POOL_THREADS_CONFIG;

/**
 * A netty thread pool associated with a particular pair of Aegis endpoints.
 */
public final class WorkerPool {
    /**
     * The configuration to use.
     */
    private final AegisConfig config;

    /**
     * The endpoint pair to manage in this worker pool.
     */
    private final EndPointPair endPointPair;

    /**
     * The logger to use.
     */
    private final Logger log;

    /**
     * A map of flow IDs to flows.
     */
    private final HashMap<Long, Flow> flows;

    /**
     * A future that is completed when we manage to bind to the local half
     * of the endpoint pair.
     */
    private final CompletableFuture<SocketAddress> bindFuture =
        new CompletableFuture<>();

    /**
     * A future that is completed when this worker pool has shut down.
     */
    private final CompletableFuture<Void> shutdownFuture =
        new CompletableFuture<>();

    /**
     * True if the worker pool is shutting down.
     */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * The netty thread pool.
     */
    private final NioEventLoopGroup group;

    private final ServerBootstrap tenantBootstrap;

    private final Bootstrap brokerBootstrap;

    /**
     * The initializer that configures a new tenant socket.
     */
    private class TenantChannelInitializer extends ChannelInitializer<Channel> {
        @Override
        protected void initChannel(Channel ch) throws Exception {
            log.debug("Initializing tenant channel {}", ch);
            ChannelPipeline pipeline = ch.pipeline();
            Flow flow = new Flow(WorkerPool.this, ch);
            synchronized (WorkerPool.this) {
                flows.put(flow.id(), flow);
            }
            pipeline.addLast(new TenantReader(flow, ch.localAddress(), ch.remoteAddress()));
            pipeline.addLast(new TenantWriter(flow, ch.localAddress(), ch.remoteAddress()));
        }
    }

    /**
     * The initializer that configures a new broker socket.
     */
    static class BrokerChannelInitializer extends ChannelInitializer<Channel> {
        private final Flow flow;

        BrokerChannelInitializer(Flow flow) {
            this.flow = flow;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            BrokerWriter writer = new BrokerWriter(flow);
            pipeline.addLast(writer);
            pipeline.addLast(new BrokerReader(flow, writer));
        }
    }

    WorkerPool(AegisConfig config, EndPointPair endPointPair) throws Exception {
        this.config = config;
        this.endPointPair = endPointPair;
        LogContext logContext = new LogContext("WorkerPool[" + endPointPair.index() + "]: ");
        this.log = logContext.logger(WorkerPool.class);
        this.flows = new HashMap<>();
        int numThreads = config.getInt(NUM_WORKER_POOL_THREADS_CONFIG);
        this.group = new NioEventLoopGroup(numThreads,
            new DefaultThreadFactory("AegisWorker-" + endPointPair.index() + "-"));
        this.tenantBootstrap = new ServerBootstrap();
        InetSocketAddress localAddress = endPointPair.tenantEndpoint().resolvedAddress();
        log.debug("Initializing WorkerPool for broker {} with localAddress {}.",
            endPointPair.index(), localAddress);
        tenantBootstrap.group(group).
            channel(NioServerSocketChannel.class).
            localAddress(localAddress).
            childHandler(new TenantChannelInitializer()).
            childOption(ChannelOption.SO_KEEPALIVE, true).
            childOption(ChannelOption.TCP_NODELAY, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000);
        tenantBootstrap.bind().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    bindFuture.complete(future.channel().localAddress());
                } else {
                    bindFuture.completeExceptionally(future.cause());
                }
            }
        });
        this.brokerBootstrap = new Bootstrap();
        brokerBootstrap.group(group).
            channel(NioSocketChannel.class).
            option(ChannelOption.SO_KEEPALIVE, true).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000).
            option(ChannelOption.TCP_NODELAY, true);
    }

    public int brokerIndex() {
        return endPointPair.index();
    }

    public EndPointPair endPointPair() {
        return endPointPair;
    }

    public Bootstrap brokerBootstrap() {
        return brokerBootstrap;
    }

    public synchronized void remove(Flow flow) {
        flows.remove(flow.id());
    }

    CompletableFuture<SocketAddress> bindFuture() {
        return bindFuture;
    }

    @SuppressWarnings("unchecked")
    CompletableFuture<Void> beginShutdown() {
        if (!shuttingDown.getAndSet(true)) {
            GenericFutureListener listener = new GenericFutureListener() {
                @Override
                public void operationComplete(Future future) throws Exception {
                    shutdownFuture.complete(null);
                }
            };
            group.shutdownGracefully().addListener(listener);
        }
        return shutdownFuture;
    }
};
