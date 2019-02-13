// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.security.auth.metadata.AuthListener;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.Scope;
import io.confluent.security.rbac.UserMetadata;
import java.util.Collections;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaAuthStoreTest {

  private final MockTime time = new MockTime();
  private final Scope clusterA = new Scope("clusterA");
  private KafkaAuthStore authStore;

  @Before
  public void setUp() throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    this.authStore = new KafkaAuthStore(rbacRoles, time, clusterA);
    authStore.configure(Collections.emptyMap());
    authStore.start();
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
  }

  @Test
  public void testListeners() throws Exception {
    assertEquals(Collections.singleton(authStore.authCache()), authStore.listeners());
    AuthListener testListener = new AuthListener() {
      @Override
      public void onRoleAssignmentAdd(RoleAssignment assignment) {
      }

      @Override
      public void onRoleAssignmentDelete(RoleAssignment assignment) {
      }

      @Override
      public void onUserUpdate(KafkaPrincipal userPrincipal, UserMetadata userMetadata) {
      }

      @Override
      public void onUserDelete(KafkaPrincipal userPrincipal) {
      }
    };
    assertTrue(authStore.addListener(testListener));
    assertEquals(Utils.mkSet(authStore.authCache(), testListener), authStore.listeners());
    assertFalse(authStore.addListener(testListener));

    assertTrue(authStore.removeListener(testListener));
    assertEquals(Collections.singleton(authStore.authCache()), authStore.listeners());
    assertFalse(authStore.removeListener(testListener));
  }
}
