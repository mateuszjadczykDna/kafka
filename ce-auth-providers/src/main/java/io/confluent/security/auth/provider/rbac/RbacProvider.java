// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.EmbeddedAuthorizer;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.provider.MetadataProvider;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.rbac.Scope;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RbacProvider implements AccessRuleProvider, GroupProvider, MetadataProvider {
  private static final Logger log = LoggerFactory.getLogger(RbacProvider.class);

  private AuthStore authStore;
  private AuthCache authCache;

  private MetadataServer metadataServer;
  private Collection<URL> metadataServerUrls;

  @Override
  public void configure(Map<String, ?> configs) {
    String scope = (String) configs.get(ConfluentAuthorizerConfig.SCOPE_PROP);
    Scope authScope;
    try {
      authScope = new Scope(scope);
    } catch (Exception e) {
      throw new ConfigException("Invalid scope for RBAC provider: " + scope, e);
    }

    if (providerName().equals(configs.get(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP))) {
      MetadataServiceConfig metadataServiceConfig = new MetadataServiceConfig(configs);
      metadataServer = metadataServiceConfig.metadataServer();
      metadataServerUrls = metadataServiceConfig.metadataServerUrls;

      Scope metadataScope = metadataServiceConfig.scope;
      if (!metadataScope.containsScope(authScope))
        throw new ConfigException(String.format("Metadata service scope %s does not contain broker scope %s",
            metadataScope, authScope));
      authScope = metadataScope;
    }
    authStore = createAuthStore(authScope, configs);
    this.authCache = authStore.authCache();
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.RBAC.name();
  }

  @Override
  public CompletionStage<Void> start() {
    CompletionStage<Void> completionStage =  authStore.startReader();
    if (metadataServer != null) {
      authStore.startService(metadataServerUrls);
      return completionStage.whenComplete((unused, exception) -> {
        if (exception == null) {
          metadataServer.start(new RbacAuthorizer(), authStore);
        }
      });
    } else {
      return completionStage;
    }
  }

  @Override
  public boolean mayDeny() {
    return false;
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             String scope) {
    return authCache.isSuperUser(new Scope(scope), userPrincipal(sessionPrincipal), groupPrincipals);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {
    return authCache.rbacRules(new Scope(scope),
                               resource,
                               userPrincipal(sessionPrincipal),
                               groupPrincipals);
  }

  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
    return authCache.groups(userPrincipal(sessionPrincipal));
  }

  @Override
  public void close() {
    log.debug("Closing RBAC provider");
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    ClientUtils.closeQuietly(metadataServer, "metadataServer", firstException);
    ClientUtils.closeQuietly(authStore, "authStore", firstException);
    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("RbacProvider could not be closed cleanly", exception);
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  // Visibility for testing
  public AuthStore authStore() {
    return authStore;
  }

  // Visibility for testing
  public AuthCache authCache() {
    return authCache;
  }

  // Allow override for testing
  protected AuthStore createAuthStore(Scope scope, Map<String, ?> configs) {
    KafkaAuthStore authStore = new KafkaAuthStore(scope);
    authStore.configure(configs);
    return authStore;
  }

  private class RbacAuthorizer extends EmbeddedAuthorizer {
    RbacAuthorizer() {
      configureProviders(Collections.singletonList(RbacProvider.this), RbacProvider.this, null);
    }
  }
}
