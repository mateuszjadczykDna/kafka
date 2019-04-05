// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Map;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.security.auth.Subject;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.kerberos.KerberosLogin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LdapContextCreator {

  private static final Logger log = LoggerFactory.getLogger(LdapContextCreator.class);

  private final LdapConfig config;
  private final Subject subject;

  public LdapContextCreator(LdapConfig config) {
    this.config = config;
    this.subject = login();
  }

  private Subject login() {
    String jaasConfigProp = LdapConfig.CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG;
    Password jaasConfig = (Password) config.values().get(jaasConfigProp);
    String authProp = LdapConfig.CONFIG_PREFIX + Context.SECURITY_AUTHENTICATION;

    // If JAAS config is provided, login regardless of authentication type
    // For GSSAPI, login using either JAAS config prop or default Configuration
    // from the login context `KafkaServer`.
    if (jaasConfig == null && !"GSSAPI".equals(config.originals().get(authProp))) {
      return new Subject();
    } else {
      try {
        JaasContext jaasContext = jaasContext(jaasConfig, "GSSAPI");

        Map<String, Object> loginConfigs = new HashMap<>();
        for (Map.Entry<String, ?> entry : config.values().entrySet()) {
          String name = entry.getKey();
          Object value = entry.getValue();
          if (name.startsWith(LdapConfig.CONFIG_PREFIX) && value != null) {
            loginConfigs
                .put(name.substring(LdapConfig.CONFIG_PREFIX.length()), value);
          }
        }
        LoginManager loginManager = LoginManager.acquireLoginManager(jaasContext, "GSSAPI",
            KerberosLogin.class, loginConfigs);
        return loginManager.subject();
      } catch (Exception e) {
        String configSource = jaasConfig != null
            ? LdapConfig.CONFIG_PREFIX + SaslConfigs.SASL_JAAS_CONFIG
            : "static JAAS configuration";
        throw new LdapException("Login using " + configSource + " failed", e);
      }
    }
  }

  Subject subject() {
    return subject;
  }

  public InitialLdapContext createLdapContext() throws IOException, NamingException {
    Hashtable<String, String> env = config.ldapContextEnvironment;
    return Subject.doAs(subject, (PrivilegedAction<InitialLdapContext>) () -> {
      try {
        return new InitialLdapContext(env, null);
      } catch (NamingException e) {
        throw new LdapException(
            "LDAP context could not be created with provided configs", e);
      }
    });
  }

  public static JaasContext jaasContext(Password jaasConfig, String mechanism) throws Exception {
    // Configuration sources in order of precedence
    //   1) JAAS configuration option: ldap.gssapi.sasl.jaas.config
    //   2) static Configuration ldap.KafkaServer
    //   3) static Configuration KafkaServer
    ListenerName listenerName = new ListenerName("ldap"); // only for static context name
    Map<String, Object> configs = jaasConfig == null ? Collections.emptyMap() :
        Collections.singletonMap(mechanism.toLowerCase(Locale.ROOT) + "."
            + SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    return JaasContext.loadServerContext(listenerName, mechanism, configs);
  }
}
