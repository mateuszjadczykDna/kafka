// Copyright 2018, Confluent Inc.

package io.confluent.aegis.proxy;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-JVM Aegis cluster used for testing.
 */
public class MiniAegisCluster {
    private static final Logger log = LoggerFactory.getLogger(MiniAegisCluster.class);

    private final List<Aegis> aegisNodes;

    MiniAegisCluster(List<Aegis> aegisNodes) {
        this.aegisNodes = Collections.unmodifiableList(aegisNodes);
    }

    public Properties createClientProps(int aegisIndex) throws Exception {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(aegisIndex));
        return props;
    }

    private String bootstrapServers(int aegisIndex) throws Exception {
        StringBuilder bld = new StringBuilder();
        Aegis aegis = aegisNodes.get(aegisIndex);
        String prefix = "";
        for (WorkerPool pool : aegis.workerPools()) {
            SocketAddress address = pool.bindFuture().get();
            @SuppressWarnings("unchecked")
            InetSocketAddress inetAddress = (InetSocketAddress) address;
            bld.append(prefix).
                append(inetAddress.getHostName()).
                append(":").
                append(inetAddress.getPort());
            prefix = ",";
        }
        return bld.toString();
    }

    public List<Aegis> nodes() {
        return aegisNodes;
    }

    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.allOf(this.aegisNodes.stream().map(
            aegis -> aegis.beginShutdown()).collect(Collectors.toList()).
                toArray(new CompletableFuture<?>[0]));

    }
}
