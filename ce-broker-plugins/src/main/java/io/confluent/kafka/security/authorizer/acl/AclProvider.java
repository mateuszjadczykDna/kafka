// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.AccessRule;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.ResourceType;
import kafka.security.auth.SimpleAclAuthorizer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;


public class AclProvider extends SimpleAclAuthorizer implements AccessRuleProvider {

  private static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private Set<KafkaPrincipal> superUsers;

  @Override
  public void configure(Map<String, ?> configs) {
    String su = (String) configs.get(ConfluentAuthorizerConfig.SUPER_USERS_PROP);
    if (su != null && !su.trim().isEmpty()) {
      String[] users = su.split(";");
      superUsers = Arrays.stream(users)
          .map(user -> SecurityUtils.parseKafkaPrincipal(user.trim()))
          .collect(Collectors.toSet());
    } else {
      superUsers = Collections.emptySet();
    }
    super.configure(configs);
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.ACL.name();
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             String scope) {
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    if (superUsers.contains(userPrincipal)) {
      log.debug("principal = {} is a super user, allowing operation without checking acls.", userPrincipal);
      return true;
    } else {
      for (KafkaPrincipal group : groupPrincipals) {
        if (superUsers.contains(group)) {
          log.debug("principal = {} belongs to super group {}, allowing operation without checking acls.",
              userPrincipal, group);
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean mayDeny() {
    return true;
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {
    ResourceType resourceType = AclMapper.kafkaResourceType(resource.resourceType());
    KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
    return JavaConversions.setAsJavaSet(getMatchingAcls(resourceType, resource.name())).stream()
        .map(AclMapper::accessRule)
        .filter(acl -> userOrGroupAcl(acl, userPrincipal, groupPrincipals))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean authorize(RequestChannel.Session session,
                           Operation operation,
                           kafka.security.auth.Resource resource) {
    throw new IllegalStateException("This provider should be used for authorization only using the AccessRuleProvider interface");
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    // Always use KafkaPrincipal instance for comparisons since super.users and ACLs are
    // instantiated as KafkaPrincipal
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  private boolean userOrGroupAcl(AccessRule rule,
                                 KafkaPrincipal userPrincipal,
                                 Set<KafkaPrincipal> groupPrincipals) {
    KafkaPrincipal aclPrincipal = rule.principal();
    return aclPrincipal.equals(userPrincipal) ||
        aclPrincipal.equals(AccessRule.WILDCARD_USER_PRINCIPAL) ||
        groupPrincipals.contains(aclPrincipal) ||
        (!groupPrincipals.isEmpty() && aclPrincipal.equals(AccessRule.WILDCARD_GROUP_PRINCIPAL));
  }
}
