// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store;

import io.confluent.kafka.security.authorizer.Operation;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.PermissionType;
import io.confluent.kafka.security.authorizer.ResourceType;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.metadata.AuthListener;
import io.confluent.security.rbac.AccessPolicy;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.Role;
import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.Scope;
import io.confluent.security.rbac.UserMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache containing authorization and authentication metadata. This is obtained from
 * a Kafka metadata topic.
 */
public class KafkaAuthCache implements AuthCache, AuthListener {
  private static final Logger log = LoggerFactory.getLogger(KafkaAuthCache.class);

  private static final String WILDCARD_HOST = "*";
  private static final NavigableMap<Resource, Set<AccessRule>> NO_RULES = Collections.emptyNavigableMap();

  private final RbacRoles rbacRoles;
  private final Scope rootScope;
  private final Map<KafkaPrincipal, UserMetadata> users;
  private final Map<Scope, Set<RoleAssignment>> roleAssignments;
  private final Map<Scope, NavigableMap<Resource, Set<AccessRule>>> rbacAccessRules;

  public KafkaAuthCache(RbacRoles rbacRoles, Scope rootScope) {
    this.rbacRoles = rbacRoles;
    this.rootScope = rootScope;
    this.users = new ConcurrentHashMap<>();
    this.roleAssignments = new ConcurrentHashMap<>();
    this.rbacAccessRules = new ConcurrentHashMap<>();
  }

  /**
   * Returns true if the provided user principal or any of the group principals has
   * `Super User` role at the specified scope.
   *
   * @param scope Scope being checked, super-users are parent level also return true
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return true if the provided principal is a super user or super group.
   */
  @Override
  public boolean isSuperUser(Scope scope,
                           KafkaPrincipal userPrincipal,
                           Set<KafkaPrincipal> groupPrincipals) {
    if (!this.rootScope.containsScope(scope))
      throw new InvalidScopeException("This authorization cache does not contain scope " + scope);

    Set<KafkaPrincipal> matchingPrincipals = matchingPrincipals(userPrincipal, groupPrincipals);
    Scope nextScope = scope;
    while (nextScope != null) {
      if (roleAssignments.getOrDefault(nextScope, Collections.emptySet()).stream()
          .filter(r -> matchingPrincipals.contains(r.principal()))
          .anyMatch(r -> rbacRoles.isSuperUser(r.role())))
        return true;
      nextScope = nextScope.parent();
    }
    return false;
  }

  /**
   * Returns the groups of the provided user principal.
   * @param userPrincipal User principal
   * @return Set of group principals of the user, which may be empty
   */
  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal userPrincipal) {
    UserMetadata user = users.get(userPrincipal);
    return user == null ? Collections.emptySet() : user.groups();
  }

  /**
   * Returns the RBAC rules corresponding to the provided principals that match
   * the specified resource.
   *
   * @param resourceScope Scope of the resource
   * @param resource Resource pattern to match
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return Set of access rules that match the principals and resource
   */
  @Override
  public Set<AccessRule> rbacRules(Scope resourceScope,
                                   Resource resource,
                                   KafkaPrincipal userPrincipal,
                                   Set<KafkaPrincipal> groupPrincipals) {

    if (!this.rootScope.containsScope(resourceScope))
      throw new InvalidScopeException("This authorization cache does not contain scope " + resourceScope);

    Set<KafkaPrincipal> matchingPrincipals = matchingPrincipals(userPrincipal, groupPrincipals);

    Set<AccessRule> resourceRules = new HashSet<>();
    Scope nextScope = resourceScope;
    while (nextScope != null) {
      NavigableMap<Resource, Set<AccessRule>> rules = rbacRules(nextScope);
      if (rules != null) {
        String resourceName = resource.name();
        ResourceType resourceType = resource.resourceType();

        addMatchingRules(rules.get(resource), resourceRules, matchingPrincipals);
        addMatchingRules(rules.get(Resource.all(resourceType)), resourceRules, matchingPrincipals);
        addMatchingRules(rules.get(RbacResource.ALL), resourceRules, matchingPrincipals);

        rules.subMap(
            new RbacResource(resourceType.name(), resourceName, PatternType.PREFIXED), true,
            new RbacResource(resourceType.name(), resourceName.substring(0, 1), PatternType.PREFIXED), true)
            .entrySet().stream()
            .filter(e -> resourceName.startsWith(e.getKey().name()))
            .forEach(e -> addMatchingRules(e.getValue(), resourceRules, matchingPrincipals));
      }
      nextScope = nextScope.parent();
    }
    return resourceRules;
  }

  private void addMatchingRules(Collection<AccessRule> inputRules,
                                Collection<AccessRule> outputRules,
                                Set<KafkaPrincipal> principals) {
    if (inputRules != null)
      inputRules.stream().filter(r -> principals.contains(r.principal())).forEach(outputRules::add);
  }

  private Set<KafkaPrincipal> matchingPrincipals(KafkaPrincipal userPrincipal,
                                                 Set<KafkaPrincipal> groupPrincipals) {
    HashSet<KafkaPrincipal> principals = new HashSet<>(groupPrincipals.size() + 1);
    principals.addAll(groupPrincipals);
    principals.add(userPrincipal);
    return principals;
  }

  @Override
  public Collection<RoleAssignment> rbacRoleAssignments(Scope scope) {
    return Collections.unmodifiableSet(roleAssignments.getOrDefault(scope, Collections.emptySet()));
  }

  @Override
  public UserMetadata userMetadata(KafkaPrincipal userPrincipal) {
    return users.get(userPrincipal);
  }

  @Override
  public Scope rootScope() {
    return rootScope;
  }

  @Override
  public RbacRoles rbacRoles() {
    return rbacRoles;
  }

  @Override
  public void onRoleAssignmentAdd(RoleAssignment assignment) {
    Scope scope = new Scope(assignment.scope());
    if (!this.rootScope.containsScope(scope))
      return;
    AccessPolicy accessPolicy = accessPolicy(assignment);
    if (accessPolicy != null) {
      roleAssignments.computeIfAbsent(scope, s -> ConcurrentHashMap.newKeySet()).add(assignment);
      NavigableMap<Resource, Set<AccessRule>> scopeRules =
          rbacAccessRules.computeIfAbsent(scope, s -> new ConcurrentSkipListMap<>());
      Map<Resource, Set<AccessRule>> rules = accessRules(assignment, accessPolicy);
      rules.forEach((r, a) ->
          scopeRules.computeIfAbsent(r, x -> ConcurrentHashMap.newKeySet()).addAll(a));
    }
  }

  @Override
  public void onRoleAssignmentDelete(RoleAssignment deletedAssignment) {
    Scope scope = new Scope(deletedAssignment.scope());
    KafkaPrincipal principal = deletedAssignment.principal();
    if (!this.rootScope.containsScope(scope))
      return;
    NavigableMap<Resource, Set<AccessRule>> scopeRules = rbacRules(scope);
    if (scopeRules != null) {
      Set<RoleAssignment> assignments = roleAssignments.get(scope);
      if (assignments != null && assignments.remove(deletedAssignment)) {
        Map<Resource, Set<AccessRule>> deletedRules = new HashMap<>();
        scopeRules.forEach((resource, rules) -> {
          Set<AccessRule> principalRules = rules.stream()
              .filter(a -> a.principal().equals(principal))
              .collect(Collectors.toSet());
          deletedRules.put(resource, principalRules);
        });
        assignments.stream()
            .filter(a -> a.principal().equals(principal))
            .flatMap(a -> accessRules(a, accessPolicy(a)).entrySet().stream())
            .forEach(e -> {
              Set<AccessRule> existing = deletedRules.get(e.getKey());
              if (existing != null)
                existing.removeAll(e.getValue());
            });
        deletedRules.forEach((resource, rules) -> {
          Set<AccessRule> resourceRules = scopeRules.get(resource);
          if (resourceRules != null) {
            resourceRules.removeAll(rules);
            if (resourceRules.isEmpty())
              scopeRules.remove(resource);
          }
        });
      }
    }
  }

  @Override
  public void onUserUpdate(KafkaPrincipal userPrincipal, UserMetadata userMetadata) {
    users.put(userPrincipal, userMetadata);
  }

  @Override
  public void onUserDelete(KafkaPrincipal userPrincipal) {
    users.remove(userPrincipal);
  }

  private AccessPolicy accessPolicy(RoleAssignment assignment) {
    Role role = rbacRoles.role(assignment.role());
    if (role == null) {
      log.error("Unknown role, ignoring role assignment {}", assignment);
      return null;
    } else {
      return role.accessPolicy();
    }
  }

  public NavigableMap<Resource, Set<AccessRule>> rbacRules(Scope scope) {
    return rbacAccessRules.getOrDefault(scope, NO_RULES);
  }

  private Map<Resource, Set<AccessRule>> accessRules(RoleAssignment roleAssignment,
                                                           AccessPolicy accessPolicy) {
    Map<Resource, Set<AccessRule>> accessRules = new HashMap<>();
    KafkaPrincipal principal = roleAssignment.principal();
    Collection<? extends Resource> resources;
    if (roleAssignment.resources().isEmpty()) {
      resources = accessPolicy.allowedOperations(ResourceType.CLUSTER).isEmpty() ?
          Collections.emptySet() : Collections.singleton(Resource.CLUSTER);

    } else {
      resources = roleAssignment.resources();
    }
    for (Resource resource : resources) {
      Set<AccessRule> resourceRules = new HashSet<>();
      for (Operation op : accessPolicy.allowedOperations(resource.resourceType())) {
        AccessRule rule = new AccessRule(principal, PermissionType.ALLOW, WILDCARD_HOST, op, String.valueOf(roleAssignment));
        resourceRules.add(rule);
      }
      accessRules.put(resource, resourceRules);
    }
    return accessRules;
  }
}