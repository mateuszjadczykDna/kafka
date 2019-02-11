// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.auth.store.clients.KafkaAuthStore;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.Scope;
import io.confluent.security.test.utils.RbacTestUtils;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RbacProviderTest {

  private final MockTime time = new MockTime();
  private final String clusterA = "testOrg/clusterA";
  private RbacProvider rbacProvider;
  private AuthCache authCache;
  private RbacResource topic = new RbacResource("Topic", "topicA", PatternType.LITERAL);

  @Before
  public void setUp() throws Exception {
    initializeRbacProvider(clusterA);
  }

  @After
  public void tearDown() {
    if (rbacProvider != null)
      rbacProvider.close();
  }

  @Test
  public void testSuperUserAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.addRoleAssignment(authCache, alice, "Super User", clusterA, null);
    assertTrue(rbacProvider.isSuperUser(alice, groups, clusterA));
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testSuperGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    RbacTestUtils.addRoleAssignment(authCache, admin, "Super User", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testClusterScopeAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.addRoleAssignment(authCache, alice, "Cluster Admin", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));

    RbacTestUtils.addRoleAssignment(authCache, alice, "Operator", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    RbacTestUtils.addRoleAssignment(authCache, alice, "Operator", clusterA, topic);
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Cluster Admin", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Operator", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Operator", clusterA, topic);
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testClusterScopeGroupAccessRules() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);

    RbacTestUtils.addRoleAssignment(authCache, admin, "Cluster Admin", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    verifyRules(accessRules(alice, Collections.emptySet(), Resource.CLUSTER));

    RbacTestUtils.addRoleAssignment(authCache, admin, "Operator", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));
    RbacTestUtils.addRoleAssignment(authCache, admin, "Operator", clusterA, topic);
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.addRoleAssignment(authCache, alice, "Operator", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Operator", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER),
        "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, admin, "Cluster Admin", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic),
        "AlterConfigs", "DescribeConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, admin, "Operator", clusterA, topic);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testLiteralResourceAccessRules() {
    verifyResourceAccessRules(new RbacResource("Topic", topic.name(), PatternType.LITERAL));
  }

  @Test
  public void testWildcardResourceAccessRules() {
    verifyResourceAccessRules(new RbacResource("Topic", "*", PatternType.LITERAL));
  }

  @Test
  public void testPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new RbacResource("Topic", "top", PatternType.PREFIXED));
  }

  @Test
  public void testSingleCharPrefixedResourceAccessRules() {
    verifyResourceAccessRules(new RbacResource("Topic", "t", PatternType.PREFIXED));
  }

  @Test
  public void testFullNamePrefixedResourceAccessRules() {
    verifyResourceAccessRules(new RbacResource("Topic", "topic", PatternType.PREFIXED));
  }

  private void verifyResourceAccessRules(RbacResource roleResource) {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal admin = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "admin");
    Set<KafkaPrincipal> groups = Collections.singleton(admin);
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();

    authCache.onRoleAssignmentAdd(RbacTestUtils.roleAssignment(alice,
        "Reader", clusterA, roleResource));
    verifyRules(accessRules(alice, emptyGroups, Resource.CLUSTER));
    verifyRules(accessRules(alice, emptyGroups, topic), "Read", "Describe");

    authCache.onRoleAssignmentAdd(RbacTestUtils.roleAssignment(admin,
        "Writer", clusterA, roleResource));
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    authCache.onRoleAssignmentAdd(RbacTestUtils.roleAssignment(alice,
        "Writer", clusterA, roleResource));
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    authCache.onRoleAssignmentDelete(RbacTestUtils.roleAssignment(admin,
        "Writer", clusterA, roleResource));
    verifyRules(accessRules(alice, groups, topic), "Read", "Describe", "Write");

    authCache.onRoleAssignmentDelete(RbacTestUtils.roleAssignment(alice,
        "Reader", clusterA, roleResource));
    verifyRules(accessRules(alice, groups, topic), "Describe", "Write");

    authCache.onRoleAssignmentDelete(RbacTestUtils.roleAssignment(alice,
        "Writer", clusterA, roleResource));
    verifyRules(accessRules(alice, groups, topic));
  }

  @Test
  public void testScopes() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.addRoleAssignment(authCache, alice, "Operator", clusterA, topic);
    verifyRules(accessRules(alice, groups, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));

    String clusterB = "testOrg/clusterB";
    RbacTestUtils.addRoleAssignment(authCache, alice, "Cluster Admin", clusterB, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER));
    verifyRules(accessRules(alice, groups, topic), "AlterConfigs", "DescribeConfigs");

  }

  @Test
  public void testProviderScope() throws Exception {
    initializeRbacProvider("testOrg");

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.addRoleAssignment(authCache, alice, "Operator", clusterA, topic);
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, Resource.CLUSTER));

    String clusterB = "testOrg/clusterB";
    RbacTestUtils.addRoleAssignment(authCache, alice, "Cluster Admin", clusterB, null);
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, Resource.CLUSTER));
    verifyRules(rbacProvider.accessRules(alice, groups, clusterA, topic), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, Resource.CLUSTER), "AlterConfigs", "DescribeConfigs");
    verifyRules(rbacProvider.accessRules(alice, groups, clusterB, topic));

    try {
      rbacProvider.accessRules(alice, groups, "anotherOrg/clusterA", Resource.CLUSTER);
      fail("Did not fail with invalid scope");
    } catch (InvalidScopeException e) {
      // Expected exception
    }
  }

  private void initializeRbacProvider(String scope) throws Exception {
    rbacProvider = new RbacProvider();
    Map<String, Object> configs = Collections.singletonMap(ConfluentAuthorizerConfig.SCOPE_PROP, scope);
    rbacProvider.configure(configs);

    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    KafkaAuthStore authStore = new KafkaAuthStore(rbacRoles, time, new Scope(scope));
    Field field = rbacProvider.getClass().getDeclaredField("authCache");
    field.setAccessible(true);
    field.set(rbacProvider, authStore.authCache());
    authCache = authStore.authCache();
  }

  private Set<AccessRule> accessRules(KafkaPrincipal userPrincipal,
                                      Set<KafkaPrincipal> groupPrincipals,
                                      Resource resource) {
    return rbacProvider.accessRules(userPrincipal, groupPrincipals, clusterA, resource);
  }

  private void verifyRules(Set<AccessRule> rules, String... expectedOps) {
    Set<String> actualOps = rules.stream().map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }
}

