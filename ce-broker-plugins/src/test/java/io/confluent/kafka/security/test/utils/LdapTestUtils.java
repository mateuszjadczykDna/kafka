// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.utils;

import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizerConfig;
import io.confluent.kafka.security.ldap.authorizer.LdapGroupManager;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;

import io.confluent.kafka.test.utils.SecurityTestUtils;
import java.util.HashMap;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import scala.Some;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class LdapTestUtils {

  public static MiniKdcWithLdapService createMiniKdcWithLdapService(
      LdapSecurityProtocol securityProtocol,
      LdapSecurityAuthentication authentication) throws Exception {
    File workDir = TestUtils.tempDir();
    Properties ldapConf = MiniKdcWithLdapService.createConfig();
    if (securityProtocol == LdapSecurityProtocol.SSL) {
      File ldapTrustStore = File.createTempFile("truststore", ".jks");
      Properties ldapSslProps = TestUtils.sslConfigs(Mode.SERVER, false, new Some<>(ldapTrustStore), "ldap", "localhost");
      MiniKdcWithLdapService.addSslConfig(ldapConf, ldapSslProps);
    }

    File keytabFile = null;
    if (authentication == LdapSecurityAuthentication.GSSAPI) {
      keytabFile = TestUtils.tempFile();
      String gssapiConfig = SecurityTestUtils.gssapiSaslJaasConfig(keytabFile, "ldap/localhost@EXAMPLE.COM", null);
      MiniKdcWithLdapService.addGssapiConfig(ldapConf, gssapiConfig);
    }

    MiniKdcWithLdapService ldapServer = new MiniKdcWithLdapService(ldapConf, workDir);
    ldapServer.start();

    if (keytabFile != null) {
      ldapServer.createPrincipals(keytabFile, "ldap/localhost");
    }
    return ldapServer;
  }

  public static void restartLdapServer(MiniKdcWithLdapService server) throws Exception {
    int port = server.ldapPort();

    long endTimeMs = System.currentTimeMillis() + 20000;
    while (System.currentTimeMillis() < endTimeMs) {
      try {
        server.startLdap(port);
        return;
      } catch (Exception e) {
        server.stopLdap();
      }
    }
    throw new IllegalStateException("LDAP server could not be restarted");
  }

  public static LdapGroupManager createLdapGroupManager(MiniKdcWithLdapService ldapServer,
      int refreshIntervalMs, Time time) {
    Map<String, Object> props = ldapAuthorizerConfigs(ldapServer, refreshIntervalMs);
    LdapAuthorizerConfig ldapConfig = new LdapAuthorizerConfig(props);
    return new LdapGroupManager(ldapConfig, time);
  }

  public static Map<String, Object> ldapAuthorizerConfigs(MiniKdcWithLdapService ldapServer, int refreshIntervalMs) {
    Map<String, Object> props = new HashMap<>();
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), LdapAuthorizer.class.getName());
    props.put(LdapAuthorizerConfig.REFRESH_INTERVAL_MS_PROP, String.valueOf(refreshIntervalMs));
    props.put(LdapAuthorizerConfig.GROUP_NAME_ATTRIBUTE_PROP, "cn");
    props.put(LdapAuthorizerConfig.GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP, "uid=(.*),ou=users,dc=example,dc=com");
    for (Map.Entry<String, String> entry : ldapServer.ldapClientConfigs().entrySet()) {
      props.put(LdapAuthorizerConfig.CONFIG_PREFIX + entry.getKey(), entry.getValue());
    }
    props.put(LdapAuthorizerConfig.CONFIG_PREFIX + SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "ldap");
    props.put(LdapAuthorizerConfig.LICENSE_PROP, LicenseTestUtils.generateLicense());
    return props;
  }

  public static void waitForUserGroups(LdapGroupManager ldapGroupManager,
      String user, String... groups) throws Exception {
    org.apache.kafka.test.TestUtils.waitForCondition(() ->
        Utils.mkSet(groups).equals(ldapGroupManager.groups(user)), "Groups not refreshed");
  }

  public static File createPrincipal(MiniKdcWithLdapService miniKdcWithLdapService, String principal) {
    try {
      File keytabFile = TestUtils.tempFile();
      miniKdcWithLdapService.createPrincipals(keytabFile, principal);
      return keytabFile;
    } catch (Exception e) {
      throw new RuntimeException("Could not create keytab", e);
    }
  }
}
