// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import io.confluent.security.auth.provider.ldap.LdapConfig;
import java.io.File;
import java.util.Properties;
import javax.naming.Context;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * Active directory service requires a pre-installed Active Directory domain controller
 * with users and groups created as necessary for the test. Keytabs of users must be
 * stored locally in a directory with each keytab in a file named USER.keytab where
 * USER is the short principal of the user. Tests must be run with System property
 * `ldap.authorizer.active.directory.keytab.dir` set to the directory containing keytabs.
 */
public class ActiveDirectoryService {

  private static final String AD_HOST = "WIN-S4QN394QEV1.COPS.IO";
  private static final int AD_PORT = 3268;
  private static final String AD_REALM = "COPS.IO";
  private static final String AD_BASE_DN = "DC=COPS,DC=IO";

  private static final String KEYTAB_DIR_PROP = "ldap.authorizer.active.directory.keytab.dir";

  private final String keytabDir;

  public ActiveDirectoryService() throws Exception {

    System.setProperty("java.security.krb5.kdc", AD_HOST);
    System.setProperty("java.security.krb5.realm", AD_REALM);

    keytabDir = System.getProperty(KEYTAB_DIR_PROP);
    if (keytabDir == null) {
      throw new IllegalArgumentException("Active directory keytab directory not specified");
    }
  }

  public User user(String name, String principal, String serviceName) {
    File keytabFile = new File(keytabDir, name + ".keytab");
    return User.gssapiUser(name, principal + "@" + AD_REALM, keytabFile, serviceName);
  }

  public Properties ldapAuthorizerConfig(User user) throws Exception {
    Properties props = new Properties();
    props.setProperty(LdapConfig.CONFIG_PREFIX + Context.PROVIDER_URL,
        "ldap://" + AD_HOST + ":" + AD_PORT + "/" + AD_BASE_DN);
    props.setProperty(LdapConfig.CONFIG_PREFIX + Context.SECURITY_AUTHENTICATION, "GSSAPI");
    props.setProperty(LdapConfig.CONFIG_PREFIX + Context.SECURITY_PRINCIPAL, user.fullPrincipal);
    props.setProperty(LdapConfig.CONFIG_PREFIX + Context.REFERRAL, "throw");
    props.setProperty(LdapConfig.CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG, user.jaasConfig);

    props.setProperty(LdapConfig.GROUP_SEARCH_BASE_PROP, "CN=Users");
    props.setProperty(LdapConfig.GROUP_NAME_ATTRIBUTE_PROP, "sAMAccountName");
    props.setProperty(LdapConfig.GROUP_MEMBER_ATTRIBUTE_PROP, "member");
    props.setProperty(LdapConfig.GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP,
        "CN=(.*),CN=Users," + AD_BASE_DN);
    props.setProperty(LdapConfig.GROUP_OBJECT_CLASS_PROP, "group");

    props.setProperty(LdapConfig.USER_SEARCH_BASE_PROP, "CN=Users");
    props.setProperty(LdapConfig.USER_NAME_ATTRIBUTE_PROP, "sAMAccountName");
    props.setProperty(LdapConfig.USER_MEMBEROF_ATTRIBUTE_PROP, "memberof");
    props.setProperty(LdapConfig.USER_MEMBEROF_ATTRIBUTE_PATTERN_PROP,
        "CN=(.*),CN=Users," + AD_BASE_DN);
    props.setProperty(LdapConfig.USER_OBJECT_CLASS_PROP, "user");

    props.setProperty(LdapConfig.REFRESH_INTERVAL_MS_PROP, "10000");
    props.setProperty(LdapConfig.CONFIG_PREFIX + SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "ldap");

    props.setProperty(LdapConfig.USER_SEARCH_FILTER_PROP,
        "(|(memberOf=CN=Kafka Admin,CN=Users,DC=COPS,DC=IO)(memberOf=CN=Kafka Developers,CN=Users,DC=COPS,DC=IO)(memberOf=CN=Kafka Testers,CN=Users,DC=COPS,DC=IO))");

    return props;
  }

  public static boolean enabled() {
    String keytabDir = System.getProperty(KEYTAB_DIR_PROP);
    return keytabDir != null && !keytabDir.isEmpty();
  }
}

