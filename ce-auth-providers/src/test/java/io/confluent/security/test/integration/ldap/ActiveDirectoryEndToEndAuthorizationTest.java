// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.ldap;

import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.provider.ldap.LdapConfig.SearchMode;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;
import io.confluent.security.test.utils.ActiveDirectoryService;
import io.confluent.security.test.utils.User;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test is disabled if the system property `ldap.authorizer.active.directory.keytab.dir` is
 * not set. It is not run as part of the build process since we don't currently have a reusable
 * Active Directory installation for use in builds.
 * <p>
 * See {@link ActiveDirectoryService} for instructions on setting up tests using AD. The class
 * must be updated if different AD host or realm is used.
 * </p><p>
 * Active Directory must be set up with the following users with groups before running the test
 * and the keytabs for the users should be available in the keytab directory specified in
 * ``ldap.authorizer.active.directory.keytab.dir`.
 * </p>
 * User -> Groups
 * <ul>
 *  <li>`kafka/localhost@COPS.IO` -> {`Kafka Admin`}</li>
 *  <li>`ldap@COPS.IO` -> {} (no groups, used for LDAP search)</li>
 *  <li>`kafkaDeveloper@COPS.IO` -> {`Kafka Developers`}</li>
 *  <li>`kafkaTester@COPS.IO` -> {`Kafka Testers`}</li>
 *  <li>`kafkaSre@COPS.IO` -> {`Kafka Developers`, `Kafka Testers`}</li>
 *  <li>`kafkaIntern@COPS.IO` -> {} (no groups)</li>
 * </ul>
 */
@RunWith(value = Parameterized.class)
public class ActiveDirectoryEndToEndAuthorizationTest extends AbstractEndToEndAuthorizationTest {

  private final SearchMode searchMode;
  private ActiveDirectoryService activeDirectoryService;

  public ActiveDirectoryEndToEndAuthorizationTest(String kafkaSaslMechanism,
      String ldapUser,
      SearchMode searchMode,
      int ldapRefreshIntervalMs) throws Exception {
    super(kafkaSaslMechanism,
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI,
        ldapUser,
        ldapRefreshIntervalMs);
    this.searchMode = searchMode;
  }

  @BeforeClass
  public static void setupClass() {
    Assume.assumeTrue(ActiveDirectoryService.enabled());
  }

  @Before
  public void setUp() throws Throwable {
    activeDirectoryService = new ActiveDirectoryService();
    super.setUp();
  }

  @Override
  protected Properties authorizerConfig() {
    try {
      Properties config = activeDirectoryService.ldapAuthorizerConfig(users.get(ldapUser));
      config.put(LdapConfig.SEARCH_MODE_PROP, searchMode.name());
      return config;
    } catch (Exception e) {
      throw new RuntimeException("Could not get active directory ldap config", e);
    }
  }

  @Override
  protected User createGssapiUser(String name, String principal) {
    String serviceName = KAFKA_SERVICE.equals(ldapUser) ? KAFKA_SERVICE : null;
    return activeDirectoryService.user(name, principal, serviceName);
  }

  @Parameterized.Parameters(name = "saslMechanism={0}, secUser={1}, searchMode={2}, "
          + "refreshInterval={3}")
  public static Collection<Object[]> data() {
    List<Object[]> values = new ArrayList<>();
    values.add(new Object[]{"SCRAM-SHA-256", LDAP_USER, SearchMode.GROUPS, 10});
    values.add(new Object[]{"GSSAPI", LDAP_USER, SearchMode.GROUPS, 0});
    values.add(new Object[]{"GSSAPI", KAFKA_SERVICE, SearchMode.USERS, 10});
    return values;
  }
}

