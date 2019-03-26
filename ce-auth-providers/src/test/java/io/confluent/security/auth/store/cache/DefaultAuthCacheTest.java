// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Resource;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.auth.store.kafka.MockAuthStore;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleBinding;
import io.confluent.security.rbac.Scope;
import io.confluent.security.rbac.UserMetadata;
import io.confluent.security.store.MetadataStoreException;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Collection;
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

public class DefaultAuthCacheTest {

  private final MockTime time = new MockTime();
  private final Scope clusterA = new Scope("clusterA");
  private RbacRoles rbacRoles;
  private KafkaAuthStore authStore;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    this.authStore = MockAuthStore.create(rbacRoles, time, clusterA, 1, 1);
    authCache = authStore.authCache();
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
  }

  @Test
  public void testClusterRoleBinding() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    RbacTestUtils.updateRoleBinding(authCache, alice, "Cluster Admin", "clusterA", Collections.emptySet());
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, Resource.CLUSTER, "DescribeConfigs", "AlterConfigs");
    assertEquals(Collections.singleton(new RoleBinding(alice, "Cluster Admin", "clusterA", null)),
        authCache.rbacRoleBindings(clusterA));
    assertEquals(Collections.emptySet(), authCache.rbacRoleBindings(new Scope("clusterB")));

    RbacTestUtils.deleteRoleBinding(authCache, alice, "Cluster Admin", "clusterA");
    assertTrue(authCache.rbacRules(clusterA).isEmpty());

    assertEquals(rbacRoles, authCache.rbacRoles());
  }

  @Test
  public void testResourceRoleBinding() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Resource topicA = new Resource("Topic", "topicA", PatternType.LITERAL);
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", "clusterA", Collections.singleton(topicA));
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, topicA, "Read", "Describe");

    KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
    Resource topicB = new Resource("Topic", "topicB", PatternType.LITERAL);
    RbacTestUtils.updateRoleBinding(authCache, bob, "Writer", "clusterA", Collections.singleton(topicB));
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB);
    verifyPermissions(bob, topicA);

    // Delete binding of unassigned role
    RbacTestUtils.deleteRoleBinding(authCache, alice, "Writer", "clusterA");
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");

    // Delete binding without specifying resource
    RbacTestUtils.deleteRoleBinding(authCache, alice, "Writer", "clusterA");
    assertEquals(2, authCache.rbacRules(clusterA).size());
    verifyPermissions(bob, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");

    // Add new topic to existing role
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", "clusterA", Utils.mkSet(topicA, topicB));
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    // Add additional role to existing topic
    RbacTestUtils.updateRoleBinding(authCache, alice, "Writer", "clusterA", Collections.singleton(topicB));
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(alice, topicB, "Read", "Describe", "Write");
    verifyPermissions(bob, topicB, "Write", "Describe");

    // Delete existing binding
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", "clusterA", Collections.singleton(topicA));
    verifyPermissions(alice, topicB, "Write", "Describe");
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    RbacTestUtils.deleteRoleBinding(authCache, alice, "Writer", "clusterA");
    verifyPermissions(alice, topicB);
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB, "Write", "Describe");

    RbacTestUtils.deleteRoleBinding(authCache, bob, "Writer", "clusterA");
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(alice, topicA, "Read", "Describe");
    verifyPermissions(bob, topicB);

    RbacTestUtils.deleteRoleBinding(authCache, alice, "Reader", "clusterA");
    assertTrue(authCache.rbacRules(clusterA).isEmpty());
  }

  @Test
  public void testUserGroups() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    UserMetadata userMetadata = new UserMetadata(Collections.emptySet());
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    KafkaPrincipal developer = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Developer");
    userMetadata = new UserMetadata(Collections.singleton(developer));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.singleton(developer), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    KafkaPrincipal tester = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Tester");
    userMetadata = new UserMetadata(Utils.mkSet(developer, tester));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Utils.mkSet(developer, tester), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    userMetadata = new UserMetadata(Collections.singleton(tester));
    RbacTestUtils.updateUser(authCache, alice, userMetadata.groups());
    assertEquals(Collections.singleton(tester), authCache.groups(alice));
    assertEquals(userMetadata, authCache.userMetadata(alice));

    RbacTestUtils.deleteUser(authCache, alice);
    assertEquals(Collections.emptySet(), authCache.groups(alice));
    assertNull(authCache.userMetadata(alice));
  }

  @Test
  public void testScopes() throws Exception {
    Scope clusterA = new Scope("org1/clusterA");
    authStore.close();
    this.authStore = MockAuthStore.create(rbacRoles, time, new Scope("org1"), 1, 1);
    authCache = authStore.authCache();

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> emptyGroups = Collections.emptySet();
    Resource topicA = new Resource("Topic", "topicA", PatternType.LITERAL);
    RbacTestUtils.updateRoleBinding(authCache, alice, "Reader", clusterA.name(), Collections.singleton(topicA));
    assertEquals(1, authCache.rbacRules(clusterA).size());
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterB = new Scope("org1/clusterB");
    RbacTestUtils.updateRoleBinding(authCache, alice, "Cluster Admin", clusterB.name(), Collections.emptySet());
    verifyPermissions(clusterB, alice, Resource.CLUSTER, "AlterConfigs", "DescribeConfigs");
    verifyPermissions(clusterA, alice, Resource.CLUSTER);
    verifyPermissions(clusterA, alice, topicA, "Read", "Describe");

    Scope clusterC = new Scope("org2/clusterC");
    RbacTestUtils.updateRoleBinding(authCache, alice, "Writer", clusterC.name(), Collections.singleton(topicA));
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

  @Test
  public void testStatusPropagation() throws Exception {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    KafkaPrincipal developer = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Developer");
    Collection<KafkaPrincipal> groups = Collections.singleton(developer);
    RbacTestUtils.updateUser(authCache, alice, groups);
    assertEquals(groups, authCache.groups(alice));

    assertEquals(MetadataStoreStatus.UNKNOWN, authCache.status(1));
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    assertEquals(MetadataStoreStatus.INITIALIZING, authCache.status(1));
    assertEquals(MetadataStoreStatus.UNKNOWN, authCache.status(2));

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZED, 1, null));
    assertEquals(MetadataStoreStatus.INITIALIZED, authCache.status(2));
    assertEquals(groups, authCache.groups(alice));

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.FAILED, 1, null));
    verifyCacheFailed();

    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZED, 1, null));
    assertEquals(groups, authCache.groups(alice));

    String error = "Test exception";
    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.FAILED, 1, error));
    try {
      authCache.groups(alice);
      fail("Exception not thrown after error");
    } catch (MetadataStoreException e) {
      assertTrue("Unexpected exception " + e, e.getMessage().contains(error));
    }
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.FAILED, 1, error));
    verifyCacheFailed();
    authCache.put(new StatusKey(2), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    verifyCacheFailed();
    authCache.put(new StatusKey(1), new StatusValue(MetadataStoreStatus.INITIALIZING, 1, null));
    assertEquals(groups, authCache.groups(alice));
  }

  private void verifyCacheFailed() {
    try {
      authCache.groups(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"));
      fail("Exception not thrown after error");
    } catch (MetadataStoreException e) {
      // Expected exception
    }
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
