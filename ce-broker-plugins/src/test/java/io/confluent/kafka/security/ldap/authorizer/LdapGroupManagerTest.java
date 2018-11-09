// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.authorizer;


import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizerConfig.SearchMode;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService;
import io.confluent.kafka.security.test.utils.LdapTestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LdapGroupManagerTest {

  private final Time time = Time.SYSTEM;
  private MiniKdcWithLdapService miniKdcWithLdapService;
  private LdapGroupManager ldapGroupManager;

  @Before
  public void setUp() throws Exception {
    miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);
  }

  @After
  public void tearDown() throws IOException {
    if (ldapGroupManager != null) {
      ldapGroupManager.close();
    }
    if (miniKdcWithLdapService != null) {
      miniKdcWithLdapService.shutdown();
    }
  }

  @Test
  public void testGroupMembers() throws Exception {
    ldapGroupManager = LdapTestUtils.createLdapGroupManager(miniKdcWithLdapService, 10, time);
    miniKdcWithLdapService.createGroup("groupA", "user1", "user2");
    miniKdcWithLdapService.createGroup("groupB", "user2", "user3");
    miniKdcWithLdapService.createGroup("groupC", "user2");
    ldapGroupManager.searchAndProcessResults();
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user1"));
    assertEquals(Collections.singleton("groupB"), ldapGroupManager.groups("user3"));
    assertEquals(Utils.mkSet("groupA", "groupB", "groupC"), ldapGroupManager.groups("user2"));
  }

  @Test
  public void testSearchWithSmallPageSizeLimit() throws Exception {
    Map<String, Object> props = LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 1000);
    props.put(LdapAuthorizerConfig.SEARCH_PAGE_SIZE_PROP, 2);
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    ldapGroupManager = new LdapGroupManager(ldapConfig, time);
    verifyGroupChanges();
  }

  @Test
  public void testSearchWithLargePageSizeLimit() throws Exception {
    Map<String, Object> props = LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 1000);
    props.put(LdapAuthorizerConfig.SEARCH_PAGE_SIZE_PROP, 100);
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    ldapGroupManager = new LdapGroupManager(ldapConfig, time);
    verifyGroupChanges();
  }

  @Test
  public void testGroupChangeWithPeriodicRefresh() throws Exception {
    ldapGroupManager = LdapTestUtils.createLdapGroupManager(miniKdcWithLdapService, 10, time);
    verifyGroupChanges();
  }

  @Test
  public void testGroupChangeWithPersistentSearch() throws Exception {
    ldapGroupManager = LdapTestUtils.createLdapGroupManager(miniKdcWithLdapService,
        LdapAuthorizerConfig.PERSISTENT_REFRESH, time);
    verifyGroupChanges();
  }

  private void verifyGroupChanges() throws Exception {
    miniKdcWithLdapService.createGroup("gA", "u1", "u2", "u3", "u4");
    miniKdcWithLdapService.createGroup("gB", "u3", "u4", "u5");
    miniKdcWithLdapService.createGroup("gC", "u5", "u6");

    ldapGroupManager.start();
    waitForUserGroups("u1", "gA");
    waitForUserGroups("u2", "gA");
    waitForUserGroups("u3", "gA", "gB");
    waitForUserGroups("u4", "gA", "gB");
    waitForUserGroups("u5", "gB", "gC");
    waitForUserGroups("u6", "gC");

    miniKdcWithLdapService.createGroup("groupA", "user1", "user2");
    waitForUserGroups("user1", "groupA");
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user2"));

    miniKdcWithLdapService.removeUserFromGroup("groupA", "user1");
    waitForUserGroups("user1");
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user2"));

    miniKdcWithLdapService.addUserToGroup("groupA", "user3");
    waitForUserGroups("user3", "groupA");
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user2"));

    miniKdcWithLdapService.createGroup("groupB", "user2", "user4");
    waitForUserGroups("user4", "groupB");
    assertEquals(Utils.mkSet("groupA", "groupB"), ldapGroupManager.groups("user2"));

    miniKdcWithLdapService.deleteGroup("groupB");
    waitForUserGroups("user2", "groupA");
    assertEquals(Collections.emptySet(), ldapGroupManager.groups("user4"));

    miniKdcWithLdapService.renameGroup("groupA", "newgroup");
    waitForUserGroups("user2", "newgroup");
    assertEquals(Collections.emptySet(), ldapGroupManager.groups("user4"));
  }

  private void waitForUserGroups(String user, String... groups) throws Exception {
    LdapTestUtils.waitForUserGroups(ldapGroupManager, user, groups);
  }

  @Test
  public void testLdapServerFailureWithPeriodicRefresh() throws Exception {
    verifyLdapServerFailure(10);
  }

  @Test
  public void testLdapServerFailureWithPersistentSearch() throws Exception {
    verifyLdapServerFailure(LdapAuthorizerConfig.PERSISTENT_REFRESH);
  }

  private void verifyLdapServerFailure(int refreshIntervalMs) throws Exception {
    createLdapGroupManagerWithRetries(refreshIntervalMs, 100);
    ldapGroupManager.start();
    miniKdcWithLdapService.createGroup("groupA", "user1", "user2");
    waitForUserGroups("user1", "groupA");
    miniKdcWithLdapService.stopLdap();
    TestUtils.waitForCondition(() -> ldapGroupManager.failed(), "LDAP failure not detected");

    LdapTestUtils.restartLdapServer(miniKdcWithLdapService);
    TestUtils.waitForCondition(() -> !ldapGroupManager.failed(), "LDAP restart not detected");
    miniKdcWithLdapService.createGroup("groupB", "user1", "user3");
    waitForUserGroups("user1", "groupA", "groupB");
    waitForUserGroups("user3", "groupB");
  }

  private void createLdapGroupManagerWithRetries(int refreshIntervalMs, int retryTimeoutMs) {
    Properties props = new Properties();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, refreshIntervalMs));
    props.setProperty(LdapAuthorizerConfig.RETRY_TIMEOUT_MS_PROP, String.valueOf(retryTimeoutMs));
    props.setProperty(LdapAuthorizerConfig.RETRY_BACKOFF_MS_PROP, "1");
    props.setProperty(LdapAuthorizerConfig.RETRY_BACKOFF_MAX_MS_PROP, "100");
    props.setProperty(LdapAuthorizerConfig.CONFIG_PREFIX + "com.sun.jndi.ldap.connect.timeout", "1000");
    props.setProperty(LdapAuthorizerConfig.CONFIG_PREFIX + "com.sun.jndi.ldap.read.timeout", "1000");
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    ldapGroupManager = new LdapGroupManager(ldapConfig, time);
  }

  @Test
  public void testLdapServerFailureDuringStartup() throws Exception {
    miniKdcWithLdapService.createGroup("groupA", "user1", "user2");
    miniKdcWithLdapService.stopLdap();

    createLdapGroupManagerWithRetries(10, 5000);
    Thread startThread = new Thread(() -> ldapGroupManager.start());
    startThread.start();
    Thread.sleep(100); // just to make sure group manager is starting
    assertTrue(startThread.isAlive());
    LdapTestUtils.restartLdapServer(miniKdcWithLdapService);
    waitForUserGroups("user1", "groupA");

    miniKdcWithLdapService.createGroup("groupB", "user1", "user3");
    waitForUserGroups("user1", "groupA", "groupB");
    waitForUserGroups("user3", "groupB");
  }

  /**
   * Apache DS has a timing window in persistent search handling. Entries created
   * just after the persistent search is started, but before the listener is created
   * to notify changes may never to returned to the client even if PersistentSearchControl
   * is created with changesOnly=false. See https://issues.apache.org/jira/browse/DIRSERVER-2257
   * for details.
   *
   * This test verifies that cache is updated in the next iteration of the search
   * when read times out after processing all changes.
   */
  @Test
  public void testGroupCreateDuringPersistentSearchStartup() throws Exception {
    startLdapGroupManagerWithPersistentSearch(2000);

    for (int i = 0; i < 50; i++) {
      miniKdcWithLdapService.createGroup("group" + i, "user" + i);
    }
    for (int i = 0; i < 50; i++) {
      waitForUserGroups("user" + i, "group" + i);
    }
  }

  @Test
  public void testGroupDeleteDuringPersistentSearchStartup() throws Exception {
    for (int i = 0; i < 50; i++) {
      miniKdcWithLdapService.createGroup("group" + i, "user" + i);
    }
    startLdapGroupManagerWithPersistentSearch(2000);

    for (int i = 0; i < 20; i++) {
      miniKdcWithLdapService.deleteGroup("group" + i);
    }
    for (int i = 20; i < 50; i++) {
      waitForUserGroups("user" + i, "group" + i);
    }
    for (int i = 0; i < 20; i++) {
      waitForUserGroups("user" + i);
    }
  }

  private void startLdapGroupManagerWithPersistentSearch(long readTimeoutMs) {
    Properties props = new Properties();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService,
        LdapAuthorizerConfig.PERSISTENT_REFRESH));
    props.setProperty(LdapAuthorizerConfig.CONFIG_PREFIX + "com.sun.jndi.ldap.read.timeout", String.valueOf(readTimeoutMs));
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    ldapGroupManager = new LdapGroupManager(ldapConfig, time);
    ldapGroupManager.start();
  }

  @Test
  public void testGroupMappingFromUsers() throws Exception {
    ldapGroupManager = createGroupManagerWithInverseMapping(10, 0);
    verifyGroupMappingFromUsers();
  }

  @Test
  public void testGroupMappingFromUsersWithSmallPageSize() throws Exception {
    ldapGroupManager = createGroupManagerWithInverseMapping(10, 2);
    verifyGroupMappingFromUsers();
  }

  @Test
  public void testGroupMappingFromUsersWithPersistentSearch() throws Exception {
    ldapGroupManager = createGroupManagerWithInverseMapping(0, 10);
    verifyGroupMappingChangesFromUsers();
  }

  @Test
  public void testGroupMappingFromUsersWithPeriodicRefresh() throws Exception {
    ldapGroupManager = createGroupManagerWithInverseMapping(10, 2);
    verifyGroupMappingChangesFromUsers();
  }

  /**
   * MiniKdc (Apache DirectoryServer doesn't currently have a virtual 'memberOf'
   * attribute to provide user->group mapping. Hence this test uses mock user/groups
   * with inverse values stored in DS.
   */
  private void verifyGroupMappingFromUsers() throws Exception {
    miniKdcWithLdapService.createGroup("user1", "groupA");
    miniKdcWithLdapService.createGroup("user2", "groupA", "groupB", "groupC");
    miniKdcWithLdapService.createGroup("user3", "groupB");
    ldapGroupManager.searchAndProcessResults();
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user1"));
    assertEquals(Collections.singleton("groupB"), ldapGroupManager.groups("user3"));
    assertEquals(Utils.mkSet("groupA", "groupB", "groupC"), ldapGroupManager.groups("user2"));

  }

  private void verifyGroupMappingChangesFromUsers() throws Exception {
    miniKdcWithLdapService.createGroup("user1", "groupA");
    miniKdcWithLdapService.createGroup("user2", "groupA", "groupB", "groupC");
    miniKdcWithLdapService.createGroup("user3", "groupB");
    ldapGroupManager.start();

    miniKdcWithLdapService.createGroup("user4", "groupB", "groupC");
    waitForUserGroups("user4", "groupB", "groupC");

    miniKdcWithLdapService.removeUserFromGroup("user2", "groupB");
    waitForUserGroups("user2", "groupA", "groupC");
    assertEquals(Utils.mkSet("groupB", "groupC"), ldapGroupManager.groups("user4"));

    miniKdcWithLdapService.addUserToGroup("user3", "groupA");
    waitForUserGroups("user3", "groupA", "groupB");
    assertEquals(Collections.singleton("groupA"), ldapGroupManager.groups("user1"));

    miniKdcWithLdapService.deleteGroup("user3");
    waitForUserGroups("user3");

    miniKdcWithLdapService.renameGroup("user4", "newuser");
    waitForUserGroups("newuser", "groupB", "groupC");
    assertEquals(Collections.emptySet(), ldapGroupManager.groups("user4"));
  }

  /**
   * MiniKdc (Apache DirectoryServer doesn't currently have a virtual 'memberOf'
   * attribute to provide user->group mapping. A group manager with mock user/groups
   * with inverse values stored in DS is used for tests that verify user->group
   * mapping.
   */
  private LdapGroupManager createGroupManagerWithInverseMapping(int refreshIntervalMs,
      int pageSize) {
    Properties props = new Properties();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, refreshIntervalMs));
    props.setProperty(LdapAuthorizerConfig.SEARCH_PAGE_SIZE_PROP, String.valueOf(pageSize));
    props.setProperty(LdapAuthorizerConfig.SEARCH_MODE_PROP, SearchMode.USERS.name());
    props.setProperty(LdapAuthorizerConfig.USER_SEARCH_BASE_PROP, "ou=groups");
    props.setProperty(LdapAuthorizerConfig.USER_OBJECT_CLASS_PROP, "groupOfNames");
    props.setProperty(LdapAuthorizerConfig.USER_NAME_ATTRIBUTE_PROP, "cn");
    props.setProperty(LdapAuthorizerConfig.USER_MEMBEROF_ATTRIBUTE_PROP, "member");
    props.setProperty(LdapAuthorizerConfig.USER_MEMBEROF_ATTRIBUTE_PATTERN_PROP,
        "uid=(.*),ou=users,dc=example,dc=com");
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    return new LdapGroupManager(ldapConfig, time);
  }
}

