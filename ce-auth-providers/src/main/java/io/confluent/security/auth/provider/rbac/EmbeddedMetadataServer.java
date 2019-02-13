// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.EmbeddedAuthorizer;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.GroupProviders;
import io.confluent.kafka.security.authorizer.provider.MetadataProvider;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.KafkaException;

public class EmbeddedMetadataServer implements MetadataProvider {
  private MetadataServer metadataServer;
  private EmbeddedAuthorizer authorizer;

  @Override
  public String providerName() {
    return "RBAC";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    MetadataServiceConfig metadataServiceConfig = new MetadataServiceConfig(configs);
    metadataServer = metadataServiceConfig.metadataServer;
    if (metadataServer != null) {
      authorizer = new EmbeddedAuthorizer();
      Map<String, Object> embeddedMetadataServerConfigs = new HashMap<>();
      embeddedMetadataServerConfigs.putAll(metadataServiceConfig.metadataServerConfigs());
      embeddedMetadataServerConfigs.put(ConfluentAuthorizerConfig.SCOPE_PROP, metadataServiceConfig.scope);
      embeddedMetadataServerConfigs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
          AccessRuleProviders.RBAC.name());
      embeddedMetadataServerConfigs.put(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP,
          GroupProviders.RBAC.name());
      embeddedMetadataServerConfigs.remove(MetadataServiceConfig.METADATA_SERVER_CLASS_PROP);
      authorizer.configure(embeddedMetadataServerConfigs);
      AccessRuleProvider rbacProvider = authorizer.accessRuleProvider(AccessRuleProviders.RBAC.name());
      if (!(rbacProvider instanceof RbacProvider)) {
        throw new IllegalStateException("Valid RBAC provider not found, provider=" + rbacProvider);
      }

      metadataServer.start(authorizer, ((RbacProvider) rbacProvider).authStore());
    }
  }

  @Override
  public void close() {
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    ClientUtils.closeQuietly(metadataServer, "metadataServer", firstException);
    ClientUtils.closeQuietly(authorizer, "authorizer", firstException);
    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("Failed to close authorizer cleanly", exception);
  }
}
