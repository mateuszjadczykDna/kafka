// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.auth.store.clients.KafkaAuthStore;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.Scope;
import io.confluent.security.rbac.UserMetadata;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AuthCacheTest {

  private final MockTime time = new MockTime();
  private final Scope clusterA = new Scope("clusterA");
  private RbacRoles rbacRoles;
  private KafkaAuthStore authStore;
  private AuthCache authCache;

  @Before
  public void setUp() throws Exception {
    rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    this.authStore = new KafkaAuthStore(rbacRoles, time, clusterA);
    authStore.configure(Collections.emptyMap());
    authStore.startReader();
    authCache = authStore.authCache();
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
  }

  @Test
  public void testClusterRoleAssignment() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    RbacTestUtils.addRoleAssignment(authCache, alice, "Cluster Admin", "clusterA", null);
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, Resource.CLUSTER, "DescribeConfigs", "AlterConfigs");

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Cluster Admin", "clusterA", null);
    assertTrue(authCache.rbacRules(clusterA).isEmpty());
  }

  @Test
  public void testResourceRoleAssignment() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    RbacResource topicA = new RbacResource("Topic", "topicA", PatternType.LITERAL);
    RbacTestUtils.addRoleAssignment(authCache, alice, "Reader", "clusterA", topicA);
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, topicA, "Read", "Describe");

    KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    RbacResource topicB = new RbacResource("Topic", "topicB", PatternType.LITERAL);
    RbacTestUtils.addRoleAssignment(authCache, bob, "Writer", "clusterA", topicB);
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB);
    verifyPermissions(bob, topicA);

    // Delete assignment of unassigned topic
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Reader", "clusterA", topicB);
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");

    // Delete assignment of unassigned role
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Writer", "clusterA", topicA);
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");

    // Delete assignment without specifying resource
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Writer", "clusterA", null);
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");

    // Add new topic to existing role
    RbacTestUtils.addRoleAssignment(authCache, alice, "Reader", "clusterA", topicB);
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    // Add additional role to existing topic
    RbacTestUtils.addRoleAssignment(authCache, alice, "Writer", "clusterA", topicB);
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB, "Read", "Describe", "Write");
    verifyPermissions(bob, topicB, "Write", "Describe");

    // Delete existing assignment
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Reader", "clusterA", topicB);
    verifyPermissions(alice, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Writer", "clusterA", topicB);
    verifyPermissions(alice, topicB);
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    RbacTestUtils.deleteRoleAssignment(authCache, bob, "Writer", "clusterA", topicB);
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB);

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Reader", "clusterA", topicA);
    assertTrue(authCache.rbacRules(clusterA).isEmpty());
  }

  @Test
  public void testUserGroups() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    UserMetadata userMetadata = new UserMetadata(Collections.emptySet());
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    authCache.onUserUpdate(alice, userMetadata);
    assertEquals(Collections.emptySet(), authCache.groups(alice));

    KafkaPrincipal developer = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Developer");
    userMetadata = new UserMetadata(Collections.singleton(developer));
    authCache.onUserUpdate(alice, userMetadata);
    assertEquals(Collections.singleton(developer), authCache.groups(alice));

    KafkaPrincipal tester = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Tester");
    userMetadata = new UserMetadata(Utils.mkSet(developer, tester));
    authCache.onUserUpdate(alice, userMetadata);
    assertEquals(Utils.mkSet(developer, tester), authCache.groups(alice));

    userMetadata = new UserMetadata(Collections.singleton(tester));
    authCache.onUserUpdate(alice, userMetadata);
    assertEquals(Collections.singleton(tester), authCache.groups(alice));

    authCache.onUserDelete(alice);
    assertEquals(Collections.emptySet(), authCache.groups(alice));
  }

  @Test
  public void testScopes() throws Exception {
    Scope clusterA = new Scope("org1/clusterA");
    this.authStore = new KafkaAuthStore(rbacRoles, time, new Scope("org1"));
    authStore.configure(Collections.emptyMap());
    authStore.startReader();
    authCache = authStore.authCache();

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    RbacResource topicA = new RbacResource("Topic", "topicA", PatternType.LITERAL);
    RbacTestUtils.addRoleAssignment(authCache, alice, "Reader", clusterA.name(), topicA);
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterB = new Scope("org1/clusterB");
    RbacTestUtils.addRoleAssignment(authCache, alice, "Cluster Admin", clusterB.name(), null);
    verifyPermissions(clusterB, alice, Resource.CLUSTER, "AlterConfigs", "DescribeConfigs");
    verifyPermissions(clusterA, alice, Resource.CLUSTER);
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterC = new Scope("org2/clusterC");
    RbacTestUtils.addRoleAssignment(authCache, alice, "Writer", clusterC.name(), topicA);
    try {
      authCache.rbacRules(clusterC, topicA, alice, emptyGroups);
      fail("Exception not thrown for unknown cluster");
    } catch (InvalidScopeException e) {
      // Expected exception
    }

    verifyPermissions(clusterB, alice, Resource.CLUSTER, "AlterConfigs", "DescribeConfigs");
    verifyPermissions(clusterA, alice, Resource.CLUSTER);
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");
  }

  private void verifyPermissions(KafkaPrincipal principal,
                                 Resource resource,
                                 String... expectedOps) {
    verifyPermissions(clusterA, principal, resource, expectedOps);
  }

  private void verifyPermissions(Scope scope,
                                 KafkaPrincipal principal,
                                 Resource resource,
                                 String... expectedOps) {
    Set<String> actualOps = authCache.rbacRules(scope, resource, principal, Collections.emptySet())
        .stream()
        .filter(r -> r.principal().equals(principal))
        .map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }
}
