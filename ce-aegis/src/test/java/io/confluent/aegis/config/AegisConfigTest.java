// Copyright 2018, Confluent Inc.

package io.confluent.aegis.config;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.confluent.aegis.config.AegisConfig.TENANT_ENDPOINTS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.junit.Assert.assertEquals;

public class AegisConfigTest {
    private static final Logger log = LoggerFactory.getLogger(AegisConfigTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testListenerSecurityProtocolMap() {
        AegisConfig config = new AegisConfig();
        Map<ListenerName, SecurityProtocol> map = config.listenerSecurityProtocolMap();
        for (SecurityProtocol proto : SecurityProtocol.values()) {
            assertEquals(proto, map.get(new ListenerName(proto.name)));
        }
    }

    @Test
    public void testResolveListenerConfig() throws Exception {
        AegisConfig config1 = new AegisConfig(
            "listener.name.broker." + SSL_KEYSTORE_LOCATION_CONFIG, "/foo/bar",
            SSL_KEYSTORE_LOCATION_CONFIG, "/baz");
        assertEquals("/baz", config1.getString(SSL_KEYSTORE_LOCATION_CONFIG));
        AegisConfig config2 = config1.resolveListenerConfig("broker");
        assertEquals("/foo/bar", config2.getString(SSL_KEYSTORE_LOCATION_CONFIG));
    }

    @Test
    public void testUnused() throws Exception {
        AegisConfig config = new AegisConfig(
            "foo.bar.baz", "quux",
            "listener.name.broker." + TENANT_ENDPOINTS_CONFIG, "baz",
            "listener.name.broker." + SSL_PROTOCOL_CONFIG, "TLS",
            SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1");
        assertEquals("foo.bar.baz," +
            "listener.name.broker." + TENANT_ENDPOINTS_CONFIG,
                new TreeSet<>(config.unused()).stream().collect(Collectors.joining(",")));
    }
}
