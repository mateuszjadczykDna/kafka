// Copyright 2018, Confluent Inc.

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniAegisClusterBuilder {
    private static final Logger log = LoggerFactory.getLogger(MiniAegisClusterBuilder.class);

    private EmbeddedKafkaCluster kafkaCluster = null;

    private List<MiniAegisBuilder> aegisBuilders = new ArrayList<>();

    public MiniAegisClusterBuilder setKafkaCluster(EmbeddedKafkaCluster kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
        return this;
    }

    public MiniAegisClusterBuilder add(MiniAegisBuilder builder) {
        aegisBuilders.add(builder);
        return this;
    }

    public MiniAegisCluster build() throws Exception {
        List<Aegis> aegisNodes = new ArrayList<Aegis>();
        boolean success = false;
        try {
            for (int aegisIndex = 0; aegisIndex < aegisBuilders.size(); aegisIndex++) {
                MiniAegisBuilder aegisBuilder = aegisBuilders.get(aegisIndex);
                Map<String, String> extraConfigs = new HashMap<>();
                if (kafkaCluster != null) {
                    addKafkaClusterExtraConfigs(aegisBuilder, extraConfigs);
                }
                aegisNodes.add(aegisBuilder.build(aegisIndex, extraConfigs));
            }
            success = true;
        } finally {
            if (!success) {
                // Clean up all the other ndoes if an Aegis node could not be created.
                for (Aegis aegis : aegisNodes) {
                    aegis.beginShutdown().get();
                }
            }
        }
        return new MiniAegisCluster(aegisNodes);
    }

    private void addKafkaClusterExtraConfigs(MiniAegisBuilder aegisBuilder,
                                             Map<String, String> extraConfigs) {
        StringBuilder tenantEndpointsBld = new StringBuilder();
        StringBuilder brokerEndpointsBld = new StringBuilder();
        String prefix = "";
        for (int brokerIndex = 0; brokerIndex < kafkaCluster.kafkas().size(); brokerIndex++) {
            tenantEndpointsBld.append(prefix).append(
                aegisBuilder.tenantListenerName().value() + "://localhost:0");
            brokerEndpointsBld.append(prefix).append(
                kafkaCluster.kafkas().get(brokerIndex).endPoint().toString());
            prefix = ",";
        }
        extraConfigs.put(AegisConfig.TENANT_ENDPOINTS_CONFIG, tenantEndpointsBld.toString());
        extraConfigs.put(AegisConfig.BROKER_ENDPOINTS_CONFIG, brokerEndpointsBld.toString());
    }
}
