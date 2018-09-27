// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.ldap;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizerConfig;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;
import io.confluent.kafka.security.test.utils.LdapTestUtils;
import io.confluent.kafka.security.test.utils.SecurityTestUtils;
import io.confluent.kafka.security.test.utils.User;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class LdapGroupEndToEndAuthorizationTest extends AbstractEndToEndAuthorizationTest {

  private MiniKdcWithLdapService miniKdcWithLdapService;

  public LdapGroupEndToEndAuthorizationTest(String kafkaSaslMechanism,
      LdapSecurityProtocol ldapSecurityProtocol,
      LdapSecurityAuthentication ldapSecurityAuthentication,
      String ldapUser,
      int ldapRefreshIntervalMs) {
    super(kafkaSaslMechanism, ldapSecurityProtocol, ldapSecurityAuthentication,
        ldapUser, ldapRefreshIntervalMs);

  }

  @Before
  public void setUp() throws Throwable {
    createLdapServer();
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    miniKdcWithLdapService.shutdown();
  }

  @Test
  public void testGroupChanges() throws Throwable {
    // Add group ACLs for dev and test topics and consumer groups
    addAcls(groupPrincipal(DEV_GROUP), DEV_TOPIC, DEV_CONSUMER_GROUP);
    addAcls(groupPrincipal(TEST_GROUP), TEST_TOPIC, TEST_CONSUMER_GROUP);

    // Add tester to dev group and verify that new group's ACLs are applied
    KafkaProducer<String, String> producer = createProducer(users.get(TESTER));
    miniKdcWithLdapService.addUserToGroup(DEV_GROUP, TESTER);
    TestUtils.waitForCondition(() -> {
      try {
        producer.partitionsFor(DEV_TOPIC);
        return true;
      } catch (AuthorizationException e) {
        return false;
      }
    }, "Groups not refreshed");
    produceConsume(users.get(TESTER), DEV_TOPIC, DEV_CONSUMER_GROUP, true);

  }

  private void createLdapServer() throws Exception {
    MiniKdcWithLdapService ldapServer =
        LdapTestUtils.createMiniKdcWithLdapService(ldapSecurityProtocol, ldapSecurityAuthentication);
    ldapServer.createGroup(ADMIN_GROUP, KAFKA_SERVICE);
    ldapServer.createGroup(DEV_GROUP, DEVELOPER, SRE);
    ldapServer.createGroup(TEST_GROUP, TESTER, SRE);
    miniKdcWithLdapService = ldapServer;
  }

  @Override
  protected Properties authorizerConfig() {
    Properties props = new Properties();
    props.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService,
        ldapRefreshIntervalMs));
    return props;
  }

  @Override
  protected User createGssapiUser(String name, String principal) {
    File keytabFile = SecurityTestUtils.createPrincipal(miniKdcWithLdapService, principal);
    String serviceName = KAFKA_SERVICE.equals(ldapUser) ? null : KAFKA_SERVICE;
    return User.gssapiUser(name, principal + "@EXAMPLE.COM", keytabFile, serviceName);
  }

  @Parameterized.Parameters(name = "kafkaSaslMechanism={0}, ldapSecurityProtocol={1}, " +
      "ldapSecurityAuthentication={2}, ldapRefreshIntervalMs={3}")
  public static Collection<Object[]> data() {
    List<Object[]> values = new ArrayList<>();
    values.add(new Object[]{
        "SCRAM-SHA-256",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.NONE,
        null,
        10
    });
    values.add(new Object[]{
        "SCRAM-SHA-256",
        LdapSecurityProtocol.SSL,
        LdapSecurityAuthentication.GSSAPI,
        LDAP_USER,
        LdapAuthorizerConfig.PERSISTENT_REFRESH
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.NONE,
        null,
        10
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI,
        LDAP_USER,
        LdapAuthorizerConfig.PERSISTENT_REFRESH
    });
    values.add(new Object[]{
        "GSSAPI",
        LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI,
        KAFKA_SERVICE,
        10
    });
    return values;
  }
}

