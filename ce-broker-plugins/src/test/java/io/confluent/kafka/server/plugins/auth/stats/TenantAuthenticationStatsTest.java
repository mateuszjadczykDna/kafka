package io.confluent.kafka.server.plugins.auth.stats;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.Assert.*;

public class TenantAuthenticationStatsTest {

  private MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
  private TenantAuthenticationStats tenantStats = TenantAuthenticationStats.instance();

  @Before
  @After
  public void cleanMBeans() {
    tenantStats.removeAllMBeans();
  }

  @Test
  public void testMBean() throws Exception {
    String tenant = "tenant1";
    String user = "userA";

    authenticate(tenant, user);
    verifyStat(mbeanName(tenant, user), 1);
    authenticate(tenant, user);
    verifyStat(mbeanName(tenant, user), 2);
  }

  @Test
  public void testMBeanNames() throws Exception{
    String tenant = "tenant\'{}\"!$:";
    String user = "user*?1";
    authenticate(tenant, user);
    verifyStat(mbeanName(ObjectName.quote(tenant), ObjectName.quote(user)), 1);
  }

  @Test
  public void testMBeanCleanup() throws Exception {
    int startMBeanCount = mbeanServer.getMBeanCount();
    Set<MultiTenantPrincipal> principals = new HashSet<>();
    principals.add(createPrincipal("tenant1", "user-1"));
    principals.add(createPrincipal("tenant1", "user-2"));
    principals.add(createPrincipal("tenant:2", "user*1"));
    principals.add(createPrincipal("tenant3", "user*1"));
    principals.add(createPrincipal("tenant*4", "user:2"));
    for (MultiTenantPrincipal principal : principals) {
      for (int i = 0; i < 5; i++) {
        authenticate(principal.tenantMetadata().tenantName, principal.getName());
      }
    }
    assertEquals(principals.size(), mbeanServer.getMBeanCount() - startMBeanCount);
    for (MultiTenantPrincipal principal : principals) {
      verifyStat(mbeanName(principal), 5);
    }

    Set<MultiTenantPrincipal> updatedPrincipals = new HashSet<>(principals);
    MultiTenantPrincipal deletedUserPrincipal = createPrincipal("tenant1", "user-2");
    MultiTenantPrincipal deletedTenantPrincipal = createPrincipal("tenant3", "user*1");
    MultiTenantPrincipal newUserPrincipal = createPrincipal("tenant1", "user-3");
    MultiTenantPrincipal newTenantPrincipal = createPrincipal("tenant5", "user*1");
    updatedPrincipals.remove(deletedUserPrincipal);
    updatedPrincipals.remove(deletedTenantPrincipal);
    updatedPrincipals.add(newUserPrincipal);
    updatedPrincipals.add(newTenantPrincipal);
    tenantStats.removeUnusedMBeans(updatedPrincipals);

    assertEquals(principals.size() - 2, mbeanServer.getMBeanCount() - startMBeanCount);
    principals.remove(deletedUserPrincipal);
    principals.remove(deletedTenantPrincipal);
    for (MultiTenantPrincipal principal : principals) {
      verifyStat(mbeanName(principal), 5);
    }
  }

  private void authenticate(String tenant, String user) {
    MultiTenantPrincipal principal = createPrincipal(tenant, user);
    tenantStats.onSuccessfulAuthentication(principal);
  }

  private ObjectName mbeanName(MultiTenantPrincipal principal) throws Exception {
    return mbeanName(tenantStats.quoteIfRequired(principal.tenantMetadata().tenantName),
        tenantStats.quoteIfRequired(principal.getName()));
  }

  private ObjectName mbeanName(String tenant, String user) throws Exception {
    return new ObjectName(String.format("%s,tenant=%s,user=%s",
        TenantAuthenticationStats.MBEAN_NAME, tenant, user));
  }

  private void verifyStat(ObjectName mbeanName, long count) throws Exception {
    assertEquals(count, mbeanServer.getAttribute(mbeanName, "Succeeded"));
  }

  private MultiTenantPrincipal createPrincipal(String tenant, String user) {
    return new MultiTenantPrincipal(user,
        new TenantMetadata(tenant, tenant));
  }
}