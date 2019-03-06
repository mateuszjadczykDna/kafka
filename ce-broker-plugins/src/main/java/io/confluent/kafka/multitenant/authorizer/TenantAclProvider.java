// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant.authorizer;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.acl.AclProvider;
import io.confluent.kafka.security.authorizer.acl.AclMapper;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.AccessRule;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.security.auth.Acl;
import kafka.security.auth.Cluster$;
import kafka.security.auth.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.JavaConversions;

/**
 * Multi-tenant authorizer that supports:
 * <ul>
 *    <li>ACLs with TenantUser:clusterId_userId as principal</li>
 *    <li>ACLs with TenantUser*:clusterId_ as wildcard prefixed principal</li>
 *    <li>ACLs with User:* as wildcard principal (e.g. for brokers or users on other listeners)</li>
 *    <li>Resource patterns with literal resource names clusterId_resourceName</li>
 *    <li>Resource patterns with prefixed resource names clusterId_resourcePrefix</li>
 *    <li>Resource patterns with tenant wildcard resource names using prefixed name clusterId_</li>
 *    <li>Resource patterns with literal wildcard resource name "*" (e.g. for broker ACLs)</li>
 *    <li>Super users configured using the configuration option `super.users`
 *        (e.g. for broker principals)</li>
 *    <li>Tenant super users with access to all tenant resources using tenant principals with
 *        {@link io.confluent.kafka.multitenant.TenantMetadata#isSuperUser} enabled.</li>
 * </ul>
 * Use of tenant prefix:
 * <ul>
 *   <li>Clients configure ACLs for User:userId</li>
 *   <li>Multi-tenant interceptor transforms User:userId to TenantUser:clusterId_userId</li>
 *   <li>ACLs are stored internally in ZooKeeper for TenantUser:clusterId_userId</li>
 *   <li>When tenants describe ACLs, prefix is removed from response by the interceptor</li>
 *   <li>Multi-tenant principal builder generates tenant principal TenantUser:clusterId_userId</li>
 *   <li>Authorizer matches TenantUser principals in ACLs obtained from ZooKeeper against
 *       TenantUser session principal generated by the principal builder.</li>
 *   <li>Non-tenant principals (e.g broker principals) are of the form User:userId in ACLs
 *       as well as session principals</li>
 * </ul>
 * Assumptions:
 * <ul>
 *   <li>All tenant ACLs have principals and resource names with tenant prefix</li>
 *   <li>All non-tenant (e.g. broker) ACLs have principals and resource names that do not
 *       contain prefix of any tenant in the cluster</li>
 *   <li>Tenant principals have type TenantUser, others have type User</li>
 * </ul>
 *
 */
public class TenantAclProvider extends AclProvider {

  private boolean authorizationDisabled;

  @Override
  public void configure(Map<String, ?> configs) {
    String maxAcls = (String) configs.get(MultiTenantAuthorizer.MAX_ACLS_PER_TENANT_PROP);
    int maxAclsPerTenant = maxAcls != null ? Integer.parseInt(maxAcls) : MultiTenantAuthorizer.ACLS_DISABLED;
    authorizationDisabled = maxAclsPerTenant == MultiTenantAuthorizer.ACLS_DISABLED;
    super.configure(configs);
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.MULTI_TENANT.name();
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             String scope) {
    return authorizationDisabled || super.isSuperUser(sessionPrincipal, groupPrincipals, scope)
      || isSuperUser(sessionPrincipal) || groupPrincipals.stream().anyMatch(this::isSuperUser);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {
    if (!groupPrincipals.isEmpty())
      throw new UnsupportedOperationException("Groups are not supported for TenantAclProvider");

    String tenantPrefix = sessionPrincipal instanceof MultiTenantPrincipal
        ? ((MultiTenantPrincipal) sessionPrincipal).tenantMetadata().tenantPrefix() : "";
    KafkaPrincipal userPrincipal = sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
    KafkaPrincipal wildcardPrincipal = tenantPrefix.isEmpty() ? Acl.WildCardPrincipal() :
        new KafkaPrincipal(MultiTenantPrincipal.TENANT_WILDCARD_USER_TYPE, tenantPrefix);

    ResourceType resourceType = AclMapper.kafkaResourceType(resource.resourceType());
    if (AclMapper.kafkaResourceType(resource.resourceType()) == Cluster$.MODULE$) {
      String prefixedCluster = tenantPrefix + resource.name();
      resource = new Resource(resource.resourceType(), prefixedCluster, resource.patternType());
    }

    return JavaConversions.setAsJavaSet(getMatchingAcls(resourceType, resource.name())).stream()
        .map(AclMapper::accessRule)
        .filter(acl -> userAcl(acl, userPrincipal, wildcardPrincipal))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean mayDeny() {
    return true;
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  private boolean isSuperUser(KafkaPrincipal userPrincipal) {
    return (userPrincipal instanceof MultiTenantPrincipal)
        && ((MultiTenantPrincipal) userPrincipal).tenantMetadata().isSuperUser;
  }

  private boolean userAcl(AccessRule rule,
                          KafkaPrincipal userPrincipal,
                          KafkaPrincipal wildcardPrincipal) {
    KafkaPrincipal aclPrincipal = rule.principal();
    return aclPrincipal.equals(userPrincipal) || aclPrincipal.equals(wildcardPrincipal);
  }
}
