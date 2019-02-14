/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.aegis.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The Aegis configuration class.
 */
public class AegisConfig extends AbstractConfig {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final ConfigDef CONFIG;

    /**
     * A regular expression used to strip the tenant or broker prefix from a configuration key.
     */
    static final Pattern PREFIX_STRIPPING_REGEX = Pattern.compile("listener\\.name\\.[a-z]*\\.");

    public static final String LISTENER_SECURITY_PROTOCOL_MAP_CONFIG = "listener.security.protocol.map";
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DOC = "A map between listener names and " +
        "security protocols.  The format is LISTENER_NAME1:SECURITY_PROTOCOL1,LISTENER_NAME2:SECURITY_PROTOCOL2,... " +
        "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
        "prefix (the listener name is lowercased) to the config name.  For example, to set a different keystore " +
        "for the INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> " +
        "would be set.";
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT =
        Arrays.stream(SecurityProtocol.values()).
            map(p -> p.toString() + ":" + p.toString()).
                collect(Collectors.joining(","));

    public static final String TENANT_ENDPOINTS_CONFIG = "tenant.endpoints";
    public static final String TENANT_ENDPOINTS_DOC = "A list of host/port pairs to set up for tenant " +
        "endpoints. This list should be in the form <code>host1:port1,host2:port2,...</code>.";

    public static final String BROKER_ENDPOINTS_CONFIG = "broker.endpoints";
    public static final String BROKER_ENDPOINTS_DOC = "A list of host/port pairs to use for contacting the " +
        "Kafka brokers.  This list should be in the form <code>host1:port1,host2:port2,...</code>.";

    public static final String NUM_WORKER_POOL_THREADS_CONFIG = "num.worker.pool.threads";
    public static final String NUM_WORKER_POOL_THREADS_DOC = "The number of worker pool threads.";
    public static final int NUM_WORKER_POOL_THREADS_DEFAULT = 10;

    /**
     * Shared configuration keys that can't be prefixed by endpoint.
     */
    static final Set<String> SHARED_CONFIGS = new HashSet<>(Arrays.asList(
        TENANT_ENDPOINTS_CONFIG,
        BROKER_ENDPOINTS_CONFIG,
        NUM_WORKER_POOL_THREADS_CONFIG
    ));

    static {
        CONFIG = new ConfigDef().define(LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
                                        Type.LIST,
                                        LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT,
                                        Importance.HIGH,
                                        LISTENER_SECURITY_PROTOCOL_MAP_DOC)
                                .define(TENANT_ENDPOINTS_CONFIG,
                                        Type.LIST,
                                        "",
                                        Importance.HIGH,
                                        TENANT_ENDPOINTS_DOC)
                                .define(BROKER_ENDPOINTS_CONFIG,
                                        Type.LIST,
                                        "",
                                        Importance.HIGH,
                                        BROKER_ENDPOINTS_DOC)
                                .define(NUM_WORKER_POOL_THREADS_CONFIG,
                                        Type.INT,
                                        NUM_WORKER_POOL_THREADS_DEFAULT,
                                        Importance.HIGH,
                                        NUM_WORKER_POOL_THREADS_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport();
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return parsedValues;
    }

    public AegisConfig(Properties props) {
        super(CONFIG, props);
    }

    public AegisConfig(Map<?, ?> props) {
        this(props, false);
    }

    /**
     * Initialize an AegisConfig based on some key/value pairs.
     *
     * @param keyValuePairs     An array in the form of alternating keys and values.
     *                          Example: key1, value1, key2, value2, ...
     */
    public AegisConfig(String... keyValuePairs) {
        super(CONFIG, keyValuePairsToMap(keyValuePairs));
    }

    private static Map<String, String> keyValuePairsToMap(String... keyValuePairs) {
        HashMap<String, String> map = new HashMap<>();
        if ((keyValuePairs.length % 2) != 0) {
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }

    /**
     * Get the configuration for a particular listener.
     * This resolves prefixed keys to their un-prefixed equivalents.
     *
     * @param listenerName  The name of the listener.
     * @return              A new configuration object.
     */
    public AegisConfig resolveListenerConfig(String listenerName) {
        return new AegisConfig(valuesWithPrefixOverride(String.
            format("listener.name.%s.", listenerName.toLowerCase(Locale.ROOT))));
    }

    protected AegisConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    @Override
    public Set<String> unused() {
        TreeSet<String> unusedKeys = new TreeSet<>();
        for (String originalKey : originals().keySet()) {
            Matcher matcher = PREFIX_STRIPPING_REGEX.matcher(originalKey);
            if (matcher.find()) {
                String strippedKey = matcher.replaceAll("");
                if (SHARED_CONFIGS.contains(strippedKey) ||
                        (!CONFIG.configKeys().keySet().contains(strippedKey))) {
                    unusedKeys.add(originalKey);
                }
            } else if (!CONFIG.configKeys().keySet().contains(originalKey)) {
                unusedKeys.add(originalKey);
            }
        }
        return unusedKeys;
    }

    /**
     * Returns a map of listener names to security protocols.
     */
    public Map<ListenerName, SecurityProtocol> listenerSecurityProtocolMap() {
        HashMap<ListenerName, SecurityProtocol> map = new HashMap<>();
        for (String elementString : getList(LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)) {
            int colonIndex = elementString.indexOf(":");
            if (colonIndex < 0) {
                throw new RuntimeException("Unable to parse listener security protocol map.  " +
                    "Can't parse listener:protocol pair " + elementString);
            }
            String listenerNameString = elementString.
                substring(0, colonIndex).toUpperCase(Locale.ROOT);
            String securityString = elementString.
                substring(colonIndex + 1).toUpperCase(Locale.ROOT);
            try {
                map.put(new ListenerName(listenerNameString),
                    SecurityProtocol.forName(securityString));
            } catch (Exception e) {
                throw new RuntimeException("Unable to parse listener security protocol map.", e);
            }
        }
        return map;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }
}
