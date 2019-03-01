// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleAssignmentKey;
import io.confluent.security.auth.store.data.RoleAssignmentValue;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.Scope;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaAuthStoreTest {

  private final Time time = new MockTime();
  private String clusterA = "testOrg/clusterA";
  private Scope scopeClusterA = new Scope(clusterA);
  private final int storeNodeId = 1;

  private MockAuthStore authStore;
  private KafkaAuthWriter authWriter;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    authStore = MockAuthStore.create(rbacRoles, time, new Scope("testOrg"), 1, storeNodeId);
    authStore.startService(authStore.urls());
    authWriter = authStore.writer();
    authCache = authStore.authCache();
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not elected");
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testConcurrency() throws Exception {
    assertTrue(authStore.isMasterWriter());

    authStore.configureDelays(1, 2);
    for (int i = 0; i < 100; i++) {
      authWriter.addRoleAssignment(principal("user" + i), "Operator", clusterA);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleAssignments(new Scope(clusterA)).size() == 100,
        "Roles not assigned");
    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i, "Operator");
    }

    authStore.configureDelays(2, 1);
    for (int i = 0; i < 100; i++) {
      authWriter.addRoleAssignment(principal("user" + i), "Cluster Admin", clusterA);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleAssignments(new Scope(clusterA)).size() == 200,
        "Roles not assigned");

    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i,  "Cluster Admin");
    }
  }

  @Test
  public void testUpdateFromDifferentNode() throws Exception {
    authStore.makeMasterWriter(storeNodeId + 1);
    assertFalse(authStore.isMasterWriter());
    int startOffset = authStore.producer.history().size();
    for (int i = 0; i < 100; i++) {
      ConsumerRecord<AuthKey, AuthValue> record = new ConsumerRecord<>(
          KafkaAuthStore.AUTH_TOPIC, 0, startOffset + i,
          new RoleAssignmentKey(principal("user" + i), "Operator", clusterA),
          new RoleAssignmentValue(Collections.emptySet()));
      authStore.consumer.addRecord(record);
    }
    TestUtils.waitForCondition(() -> authCache.rbacRoleAssignments(new Scope(clusterA)).size() == 100,
        "Roles not assigned");
    for (int i = 0; i < 100; i++) {
      verifyRole("user" + i, "Operator");
    }
  }

  private KafkaPrincipal principal(String userName) {
    return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);
  }

  private void verifyRole(String userName, String role) {
    RoleAssignment assignment =
        new RoleAssignment(principal(userName), role, clusterA, Collections.emptySet());
    assertTrue("Missing role for " + userName,
        authCache.rbacRoleAssignments(scopeClusterA).contains(assignment));
  }

}
