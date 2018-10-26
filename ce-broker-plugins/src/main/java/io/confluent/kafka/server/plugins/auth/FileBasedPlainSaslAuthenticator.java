// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.TenantMetadata;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.util.List;
import java.util.Map;

/**
 * Authenticate users based on YAML config file which is periodically reloaded
 * <p>
 * Since we have to use JAAS, the JAAS config just points to
 * the YAML config file.
 * </p>
 */
public class FileBasedPlainSaslAuthenticator implements SaslAuthenticator {
  static final String JAAS_ENTRY_CONFIG = "config_path";
  static final String JAAS_ENTRY_REFRESH_MS = "refresh_ms";
  private static final String AUTHENTICATION_FAILED_ERROR =
      "Authentication failed: Invalid username or password";
  private static final Logger logger =
      LoggerFactory.getLogger(FileBasedPlainSaslAuthenticator.class);

  private static final String SASL_MECHANISM_PLAIN = "PLAIN";
  private SecretsLoader loader;
  private static final int BCRYPT_PASSSWORD_CACHE_SIZE = 2048;
  private static final LRUCache<String, String> bcryptPasswordCache =
      new LRUCache<>(BCRYPT_PASSSWORD_CACHE_SIZE);

  @Override
  public void initialize(List<AppConfigurationEntry>  jaasContextEntries) {
    String configFilePath = configEntryOption(jaasContextEntries,
            JAAS_ENTRY_CONFIG, FileBasedLoginModule.class.getName());
    long refreshMs = Long.parseLong(configEntryOption(jaasContextEntries,
            JAAS_ENTRY_REFRESH_MS, FileBasedLoginModule.class.getName())
    );
    loader = new SecretsLoader(
        configFilePath,
        refreshMs
    );
  }

  /**
   * Authenticate user via username/password
   * @param username username
   * @param password password
   * @returns {@link MultiTenantPrincipal} containing authorization id and tenant
   * @throws SaslAuthenticationException if authentication fails.
   *         Note that the error string in the SaslAuthenticationException
   *         is returned to the client so do not leak information in it.
   * @throws SaslException if any unexpected errors are encountered
   */
  @Override
  public MultiTenantPrincipal authenticate(String username, String password)
      throws SaslException, SaslAuthenticationException {
    try {
      Map<String, KeyConfigEntry> passwords = loader.get();
      if (passwords.containsKey(username)) {
        KeyConfigEntry entry = passwords.get(username);
        if (entry.saslMechanism.equals(SASL_MECHANISM_PLAIN)) {
          switch (entry.hashFunction) {
            case "none":
              if (!entry.hashedSecret.equals(password)) {
                logger.trace("Bad password for user {}", username);
                throw new SaslAuthenticationException(AUTHENTICATION_FAILED_ERROR);
              }
              break;
            case "bcrypt":
              authenticateBcrypt(entry.hashedSecret, username, password);
              break;
            default:
              logger.error("Unknown hash function: {} for user {}", entry.hashFunction,
                  username);
              throw new SaslAuthenticationException(AUTHENTICATION_FAILED_ERROR);
          }

          // At the moment, we use the same value for both the tenant name and the clusterId.
          // This makes it easy to associate clusters in applications like C3 with the
          // corresponding customer cluster.
          TenantMetadata tenantMetadata = new TenantMetadata.Builder(entry.logicalClusterId)
              .superUser(!entry.isServiceAccount()).build();
          return new MultiTenantPrincipal(entry.userId, tenantMetadata);
        } else {
          logger.error("Wrong SASL mechanism {} for user {}", entry.saslMechanism, username);
          throw new SaslAuthenticationException(AUTHENTICATION_FAILED_ERROR);
        }
      } else {
        logger.trace("Unknown user {}", username);
        throw new SaslAuthenticationException(AUTHENTICATION_FAILED_ERROR);
      }
    } catch (SaslAuthenticationException e) {
      throw e;
    } catch (Exception e) {
      logger.error("Unexpected exception during authentication for user {}", username, e);
      throw new SaslException("Authentication failed: Unexpected exception", e);
    }
  }

  private void authenticateBcrypt(String hashedSecret, String username, String password) {
    String hash = null;
    synchronized (bcryptPasswordCache) {
      hash = bcryptPasswordCache.get(password);
    }
    if (hashedSecret.equals(hash)) {
      return;
    }
    if (!BCrypt.checkpw(password, hashedSecret)) {
      logger.trace("Bad password for user {}", username);
      throw new SaslAuthenticationException(AUTHENTICATION_FAILED_ERROR);
    }
    synchronized (bcryptPasswordCache) {
      bcryptPasswordCache.put(password, hashedSecret);
    }
  }

  // Visibility for testing
  static String configEntryOption(List<AppConfigurationEntry> configurationEntries,
                                         String key, String loginModuleName) {
    for (AppConfigurationEntry entry : configurationEntries) {
      if (loginModuleName != null && !loginModuleName.equals(entry.getLoginModuleName())) {
        continue;
      }
      Object val = entry.getOptions().get(key);
      if (val != null) {
        return (String) val;
      }
    }
    return null;
  }
}
