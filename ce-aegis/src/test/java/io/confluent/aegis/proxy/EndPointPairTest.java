// Copyright 2018, Confluent Inc.

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;
import io.confluent.common.EndPoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.aegis.config.AegisConfig.BROKER_ENDPOINTS_CONFIG;
import static io.confluent.aegis.config.AegisConfig.TENANT_ENDPOINTS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EndPointPairTest {
    private static final Logger log = LoggerFactory.getLogger(EndPointPairTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    private static void expectException(String text, Runnable runnable) throws Exception {
        try {
            runnable.run();
            fail("Expected to get an exception containing '" + text + "'");
        } catch (Exception e) {
            assertTrue("Got an exception " + e.getMessage() + " but it did " +
                "not contain the expected text '" + text + "'",
                e.getMessage().contains(text));
        }
    }

    @Test
    public void testParseEndpoints() throws Exception {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TENANT_ENDPOINTS_CONFIG, "PLAINTEXT://localhost:8080,SSL://localhost:9090");
        configMap.put(BROKER_ENDPOINTS_CONFIG, "PLAINTEXT://example.com:8080,SSL://example.com:9090");
        List<EndPointPair> pairs = EndPointPair.createPairs(new AegisConfig(configMap));
        assertEquals(2, pairs.size());
        assertEquals(0, pairs.get(0).index());
        assertEquals(new EndPoint("localhost", 8080, SecurityProtocol.PLAINTEXT),
            pairs.get(0).tenantEndpoint());
        assertEquals(new EndPoint("example.com", 8080, SecurityProtocol.PLAINTEXT),
            pairs.get(0).brokerEndpoint());
        assertEquals(1, pairs.get(1).index());
        assertEquals(new EndPoint("localhost", 9090, SecurityProtocol.SSL),
            pairs.get(1).tenantEndpoint());
        assertEquals(new EndPoint("example.com", 9090, SecurityProtocol.SSL),
            pairs.get(1).brokerEndpoint());
    }

    @Test
    public void testInvalidConfiguration() throws Exception {
        expectException("You must supply at least one tenant endpoint", () -> {
            EndPointPair.createPairs(new AegisConfig(Collections.emptyMap()));
        });
        expectException("You must supply at least one tenant endpoint", () -> {
            EndPointPair.createPairs(new AegisConfig(Collections.
                singletonMap(TENANT_ENDPOINTS_CONFIG, "")));
        });
        expectException("The number of each must match", () -> {
            Map<String, String> configMap = new HashMap<>();
            configMap.put(TENANT_ENDPOINTS_CONFIG, "PLAINTEXT://localhost:8080");
            configMap.put(BROKER_ENDPOINTS_CONFIG, "PLAINTEXT://example.com:8080,SSL://example.com:9090");
            EndPointPair.createPairs(new AegisConfig(configMap));
        });
        expectException("Unable to parse tenant endpoints", () -> {
            Map<String, String> configMap = new HashMap<>();
            configMap.put(TENANT_ENDPOINTS_CONFIG, "PLAINTEXT:/localhost:8080");
            configMap.put(BROKER_ENDPOINTS_CONFIG, "PLAINTEXT://example.com:8080");
            EndPointPair.createPairs(new AegisConfig(configMap));
        });
        expectException("Unable to parse broker endpoints", () -> {
            Map<String, String> configMap = new HashMap<>();
            configMap.put(TENANT_ENDPOINTS_CONFIG, "PLAINTEXT://localhost:8080");
            configMap.put(BROKER_ENDPOINTS_CONFIG, "example.com:8080");
            EndPointPair.createPairs(new AegisConfig(configMap));
        });
    }
}
