// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class SuperUserProvider implements AccessRuleProvider {

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal, Set<KafkaPrincipal> groupPrincipals, String scope) {
    return true;
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {
    return Collections.emptySet();
  }

  @Override
  public boolean mayDeny() {
    return false;
  }

  @Override
  public String providerName() {
    return "SUPER_USERS";
  }

  @Override
  public void close() throws IOException {
  }
}
