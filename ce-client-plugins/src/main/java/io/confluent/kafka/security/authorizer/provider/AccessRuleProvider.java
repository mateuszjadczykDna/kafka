// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.provider;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.Resource;
import java.io.Closeable;
import java.util.Set;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Interface used by providers of access rules used for authorization.
 * Access rules may be derived from ACLs, RBAC policies etc.
 */
public interface AccessRuleProvider extends Configurable, Closeable {

  /**
   * Returns true if either the session's user principal or one of the provided group
   * principals is a super user. All operations are authorized for super-users without
   * checking any access rules.
   *
   * @param sessionPrincipal User principal from the Session
   * @param groupPrincipals List of group principals of the user, which may be empty
   * @param scope Scope of resource being access
   * @return true if super-user or super-group
   */
  boolean isSuperUser(KafkaPrincipal sessionPrincipal, Set<KafkaPrincipal> groupPrincipals, String scope);

  /**
   * Returns the set of access rules for the user and group principals that match the provided
   * resource.
   *
   * @param sessionPrincipal User principal from the Session
   * @param groupPrincipals List of group principals of the user, which may be empty
   * @param scope Scope of resource
   * @param resource Resource being accessed
   * @return Set of matching rules
   */
  Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                              Set<KafkaPrincipal> groupPrincipals,
                              String scope,
                              Resource resource);

  /**
   * Returns true if this provider supports DENY rules. If false, this provider's rules are
   * not retrieved if an ALLOW rule was found on another provider.
   * @return Boolean indicating if the provider supports DENY rules.
   */
  boolean mayDeny();

  /**
   * Returns the name of the provider.
   * @return Provider name
   */
  String providerName();
}
