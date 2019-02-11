// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.auth.store.clients.KafkaAuthStore;
import io.confluent.security.rbac.Scope;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class RbacProvider implements AccessRuleProvider, GroupProvider {

  private KafkaAuthStore authStore;
  private AuthCache authCache;

  @Override
  public void configure(Map<String, ?> configs) {
    String scope = (String) configs.get(ConfluentAuthorizerConfig.SCOPE_PROP);
    if (scope == null || scope.isEmpty())
      throw new ConfigException("Scope must be non-empty for RBAC provider");
    authStore = new KafkaAuthStore(new Scope(scope));
    authStore.startReader();
    this.authCache = authStore.authCache();
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.RBAC.name();
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
    if (authStore != null) {
      authStore.close();
    }
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }
}
