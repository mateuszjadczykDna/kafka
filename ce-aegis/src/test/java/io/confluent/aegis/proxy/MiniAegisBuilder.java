// Copyright 2018, Confluent Inc.

package io.confluent.aegis.proxy;

import io.confluent.aegis.config.AegisConfig;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;

public class MiniAegisBuilder {
    private final AegisConfig config;
    private ListenerName tenantListenerName = new ListenerName("PLAINTEXT");

    public MiniAegisBuilder(AegisConfig config) {
        this.config = config;
    }

    public MiniAegisBuilder setTenantListenerName(ListenerName tenantListenerName) {
        this.tenantListenerName = tenantListenerName;
        return this;
    }

    public ListenerName tenantListenerName() {
        return tenantListenerName;
    }

    Aegis build(int aegisIndex, Map<String, String> extraConfigs) throws Exception {
        Map<String, String> entries = new HashMap<>(config.originalsStrings());
        entries.putAll(extraConfigs);
        LogContext logContext =
            new LogContext(String.format("[Aegis%02d] ", aegisIndex));
        return new Aegis(logContext, new AegisConfig(entries));
    }
}
