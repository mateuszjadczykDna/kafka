// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * The main class for the Aegis proxy.
 */
public final class Aegis {
    static {
        InternalLoggerFactory.setDefaultFactory(Log4JLoggerFactory.INSTANCE);
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The configuration.
     */
    private final AegisConfig config;

    /**
     * The pairs of endpoints to manage.
     */
    private final List<EndPointPair> endPointPairs;

    /**
     * A future that will be completed when the proxy is closed.
     */
    private final CompletableFuture<Void> shutdownFuture;

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * The thread pool for inbound tenant connections.
     */
    private final List<WorkerPool> workerPools;

    public Aegis(LogContext logContext, AegisConfig config) throws Exception {
        this.log = logContext.logger(Aegis.class);
        this.config = config;
        this.endPointPairs = EndPointPair.createPairs(config);
        this.shutdownFuture = new CompletableFuture<>();
        this.workerPools = new ArrayList<>();
        for (EndPointPair endPointPair : endPointPairs) {
            this.workerPools.add(new WorkerPool(config, endPointPair));
        }
        log.info("Starting Aegis worker pools.");
        waitForWorkerPools();
    }

    private void waitForWorkerPools() throws Exception {
        for (final WorkerPool pool : workerPools) {
            pool.bindFuture().whenComplete(new BiConsumer<SocketAddress, Throwable>() {
                @Override
                public void accept(SocketAddress socketAddress, Throwable t) {
                    if (t == null) {
                        log.info("Listening on {} for broker {}",
                            socketAddress.toString(), pool.brokerIndex());
                    } else {
                        log.error("Failed to open tenant endpoint {} on {}",
                            pool.endPointPair().index(), pool.endPointPair().tenantEndpoint(), t);
                        pool.beginShutdown();
                        throw new CompletionException(t);
                    }
                }
            }).get();
        }
    }

    // VisibleForTesting
    List<WorkerPool> workerPools() {
        return workerPools;
    }

    public CompletableFuture<Void> beginShutdown() {
        if (!shuttingDown.getAndSet(true)) {
            final AtomicInteger remainingPools = new AtomicInteger(workerPools.size());
            for (WorkerPool pool : workerPools) {
                pool.beginShutdown().thenApply(new Function<Void, Void>() {
                    @Override
                    public Void apply(Void aVoid) {
                        if (remainingPools.decrementAndGet() == 0) {
                            log.debug("Shutdown completed.");
                            shutdownFuture.complete(null);
                        }
                        return null;
                    }
                });
            }
        }
        return shutdownFuture;
    }

    private void waitForShutdown() throws Exception {
        shutdownFuture.get();
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("aegis")
            .defaultHelp(true)
            .description("The Aegis Proxy");
        parser.addArgument("conf")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("conf")
            .metavar("CONF")
            .help("The Aegis configuration file to use.");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        String configPath = res.getString("conf");
        Properties props = Utils.loadProps(configPath);
        AegisConfig conf = new AegisConfig(props);
        Aegis aegis = new Aegis(new LogContext(), conf);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                aegis.log.warn("Running Aegis shutdown hook.");
                try {
                    aegis.beginShutdown().get();
                } catch (Exception e) {
                    aegis.log.error("Got exception while running Aegis shutdown hook.", e);
                }
            }
        });
        aegis.waitForShutdown();
    }
};
