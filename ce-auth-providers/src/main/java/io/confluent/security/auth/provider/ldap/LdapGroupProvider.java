// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.GroupProviders;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.AccessRule;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;

public class LdapGroupProvider implements GroupProvider {

  private final Time time;
  private LdapGroupManager groupManager;

  public LdapGroupProvider() {
    this(Time.SYSTEM);
  }

  public LdapGroupProvider(Time time) {
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    LdapAuthorizerConfig authorizerConfig = new LdapAuthorizerConfig(configs);
    groupManager = new LdapGroupManager(authorizerConfig, time);
    groupManager.start();
  }

  @Override
  public String providerName() {
    return GroupProviders.LDAP.name();
  }

  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
    if (groupManager == null)
      return Collections.emptySet();
    else {
      Set<String> groups = groupManager.groups(sessionPrincipal.getName());
      return groups.stream()
          .map(group -> new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, group))
          .collect(Collectors.toSet());
    }
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public void close() {
    if (groupManager != null) {
      groupManager.close();
    }
  }

  public LdapGroupManager ldapGroupManager() {
    return groupManager;
  }
}
