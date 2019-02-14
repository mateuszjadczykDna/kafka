// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;
import io.confluent.common.EndPoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A pair of endpoints.  One is the local endpoint the tenants should connect
 * to; the other is the remote broker endpoint the messages should be sent to.
 */
public final class EndPointPair {
    /**
     * Create the configured EndPointPair objects.
     *
     * @param conf  The Aegis configuration.
     * @return      The endpoints.
     */
    static List<EndPointPair> createPairs(AegisConfig conf) {
        List<String> tenantEndpointStrings =
            conf.getList(AegisConfig.TENANT_ENDPOINTS_CONFIG);
        if (tenantEndpointStrings == null || tenantEndpointStrings.isEmpty()) {
            throw new RuntimeException("You must supply at least one tenant endpoint in " +
                AegisConfig.TENANT_ENDPOINTS_CONFIG);
        }
        List<String> brokerEndpointStrings =
            conf.getList(AegisConfig.BROKER_ENDPOINTS_CONFIG);
        if (brokerEndpointStrings == null || brokerEndpointStrings.isEmpty()) {
            throw new RuntimeException("You must supply at least one broker endpoint in " +
                AegisConfig.BROKER_ENDPOINTS_CONFIG);
        }
        if (tenantEndpointStrings.size() != brokerEndpointStrings.size()) {
            throw new RuntimeException(String.format(
                "You supplied %d tenant endpoints, but %d broker endpoints.  " +
                "The number of each must match.",
                tenantEndpointStrings.size(),
                brokerEndpointStrings.size()));
        }
        Map<ListenerName, SecurityProtocol> securityMap = conf.listenerSecurityProtocolMap();
        List<EndPoint> tenantEndpoints;
        try {
            tenantEndpoints = tenantEndpointStrings.stream().
                map(str -> EndPoint.parse(str, securityMap)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse tenant endpoints " + tenantEndpointStrings, e);
        }
        List<EndPoint> brokerEndpoints;
        try {
            brokerEndpoints = brokerEndpointStrings.stream().
                map(str -> EndPoint.parse(str, securityMap)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse broker endpoints " + brokerEndpointStrings, e);
        }
        List<EndPoint> allEndpoints = new ArrayList<>();
        allEndpoints.addAll(tenantEndpoints);
        allEndpoints.addAll(brokerEndpoints);
        Map<ListenerName, AegisConfig> listenerConfigs = new HashMap<>();
        for (EndPoint endPoint : allEndpoints) {
            if (!listenerConfigs.containsKey(endPoint.listenerName())) {
                listenerConfigs.put(endPoint.listenerName(),
                    conf.resolveListenerConfig(endPoint.listenerName().value()));
            }
        }
        ArrayList<EndPointPair> endpoints = new ArrayList<>(tenantEndpoints.size());
        for (int i = 0; i < brokerEndpointStrings.size(); i++) {
            endpoints.add(new EndPointPair(i,
                tenantEndpoints.get(i),
                listenerConfigs.get(tenantEndpoints.get(i).listenerName()),
                brokerEndpoints.get(i),
                listenerConfigs.get(brokerEndpoints.get(i).listenerName())));
        }
        return endpoints;
    }

    /**
     * The endpoint pair index.
     */
    private final int index;

    /**
     * The endpoint that Aegis will listen on.  The tenants will connect to this
     * endpoint.
     */
    private final EndPoint tenantEndpoint;

    /**
     * The configuration for the tenant endpoint.
     */
    private final AegisConfig tenantEndpointConfig;

    /**
     * The broker endpoint that Aegis will connect to.
     */
    private final EndPoint brokerEndpoint;

    /**
     * The configuration for the broker endpoint.
     */
    private final AegisConfig brokerEndpointConfig;

    public EndPointPair(int index,
                        EndPoint tenantEndpoint,
                        AegisConfig tenantEndpointConfig,
                        EndPoint brokerEndpoint,
                        AegisConfig brokerEndpointConfig) {
        this.index = index;
        this.tenantEndpoint = tenantEndpoint;
        this.tenantEndpointConfig = tenantEndpointConfig;
        this.brokerEndpoint = brokerEndpoint;
        this.brokerEndpointConfig = brokerEndpointConfig;
    }

    public int index() {
        return index;
    }

    public EndPoint tenantEndpoint() {
        return tenantEndpoint;
    }

    public AegisConfig tenantEndpointConfig() {
        return tenantEndpointConfig;
    }

    public EndPoint brokerEndpoint() {
        return brokerEndpoint;
    }

    public AegisConfig brokerEndpointConfig() {
        return brokerEndpointConfig;
    }
};
