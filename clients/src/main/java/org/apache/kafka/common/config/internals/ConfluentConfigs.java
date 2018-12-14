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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.interceptor.BrokerInterceptor;
import org.apache.kafka.server.interceptor.DefaultBrokerInterceptor;
import org.apache.kafka.server.multitenant.MultiTenantMetadata;

import java.util.Map;

public class ConfluentConfigs {
    public static final String BROKER_INTERCEPTOR_CLASS_CONFIG = "broker.interceptor.class";
    public static final Class<?> BROKER_INTERCEPTOR_CLASS_DEFAULT = DefaultBrokerInterceptor.class;
    public static final String MULTITENANT_METADATA_CLASS_CONFIG = "multitenant.metadata.class";
    public static final String MULTITENANT_METADATA_CLASS_DEFAULT = null;
    public static final String MULTITENANT_METADATA_DIR_CONFIG = "multitenant.metadata.dir";
    public static final String MULTITENANT_METADATA_DIR_DEFAULT = null;


    public static BrokerInterceptor buildBrokerInterceptor(Mode mode, Map<String, ?> configs) {
        if (mode == Mode.CLIENT)
            return null;

        BrokerInterceptor interceptor = new DefaultBrokerInterceptor();
        if (configs.containsKey(BROKER_INTERCEPTOR_CLASS_CONFIG)) {
            @SuppressWarnings("unchecked")
            Class<? extends BrokerInterceptor> interceptorClass =
                    (Class<? extends BrokerInterceptor>) configs.get(BROKER_INTERCEPTOR_CLASS_CONFIG);
            interceptor = Utils.newInstance(interceptorClass);
        }
        interceptor.configure(configs);
        return interceptor;
    }

    public static MultiTenantMetadata buildMultitenantMetadata(Map<String, ?> configs) {
        MultiTenantMetadata meta = null;
        if (configs.get(MULTITENANT_METADATA_CLASS_CONFIG) != null) {
            @SuppressWarnings("unchecked")
            Class<? extends MultiTenantMetadata> multitenantMetadataClass =
                (Class<? extends MultiTenantMetadata>) configs.get(MULTITENANT_METADATA_CLASS_CONFIG);
            meta = Utils.newInstance(multitenantMetadataClass);
            meta.configure(configs);
        }
        return meta;
    }
}
