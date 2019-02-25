// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.provider;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class BuiltInAuthProviders {

    public enum BasicAuthCredentialProviders {
        USER_INFO, // UserInfo credential provider
        NONE,      // Basic authentication is disabled
    }

    public static Set<String> builtInBasicAuthCredentialProviders() {
        return Utils.mkSet(BasicAuthCredentialProviders.values()).stream()
                .map(BasicAuthCredentialProviders::name).collect(Collectors.toSet());
    }

    public static BasicAuthCredentialProvider loadBasicAuthCredentialProvider(String name) {
        if (name.equals(BasicAuthCredentialProviders.NONE.name()))
            return new EmptyBasicAuthCredentialProvider();

        BasicAuthCredentialProvider basicAuthCredentialProvider = null;
        ServiceLoader<BasicAuthCredentialProvider> providers = ServiceLoader.load(BasicAuthCredentialProvider.class);
        for (BasicAuthCredentialProvider provider : providers) {
            if (provider.providerName().equals(name)) {
                basicAuthCredentialProvider = provider;
                break;
            }
        }
        if (basicAuthCredentialProvider == null)
            throw new ConfigException("BasicAuthCredentialProvider not found for " + name);
        return basicAuthCredentialProvider;
    }

    private static class EmptyBasicAuthCredentialProvider implements BasicAuthCredentialProvider {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public String providerName() {
            return BasicAuthCredentialProviders.NONE.name();
        }

        @Override
        public String getUserInfo() {
            return null;
        }
    }
}