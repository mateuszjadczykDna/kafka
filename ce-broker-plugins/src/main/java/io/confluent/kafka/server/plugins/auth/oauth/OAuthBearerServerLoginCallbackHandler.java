// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.oauth;

import io.confluent.common.security.utils.PemUtils;
import io.confluent.kafka.common.multitenant.oauth.OAuthBearerJwsToken;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * A {@code CallbackHandler} for the OAuthLoginModule of a Kafka Broker.
 * It works as a work-around for KAFKA-7462 and validates that a valid PEM public key is given.
 *
 * <pre>
 *  org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      publicKeyPath="/tmp/key.pem"
 * </pre>
 *
 * <p>This class should be explicitly set via the
 * {@code listener.name.XXX.YYY.sasl.login.callback.handler.class} configuration property
 */
public class OAuthBearerServerLoginCallbackHandler implements AuthenticateCallbackHandler {
  private final Logger log = LoggerFactory.getLogger(
          OAuthBearerServerLoginCallbackHandler.class);
  private boolean configured = false;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, String saslMechanism,
                        List<AppConfigurationEntry> jaasConfigEntries) {
    if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException(
              String.format("Unexpected SASL mechanism: %s", saslMechanism));
    }
    if (Objects.requireNonNull(jaasConfigEntries).size() != 1
            || jaasConfigEntries.get(0) == null) {
      throw new IllegalArgumentException(
              String.format(
                      "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                      jaasConfigEntries.size()));
    }

    Map<String, String> moduleOptions = Collections.unmodifiableMap(
            (Map<String, String>) jaasConfigEntries.get(0).getOptions());

    try {
      String publicKeyPath = moduleOptions.get("publicKeyPath");
      if (publicKeyPath == null) {
        log.error("No publicKeyPath was provided in the JAAS config!");
        throw new ConfigException("publicKeyPath option must be set in JAAS config!");
      }

      PemUtils.loadPublicKey(new FileInputStream(publicKeyPath));
    } catch (IOException e) {
      String errMsg = String.format("Could not load the public key from %s",
              moduleOptions.get("publicKeyPath"));
      log.error(errMsg);
      throw new ConfigException(errMsg, e);
    }

    configured = true;
  }

  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    if (!configured) {
      throw new IllegalStateException("Callback handler not configured");
    }

    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerTokenCallback) {
        attachAuthToken((OAuthBearerTokenCallback) callback);
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override
  public void close() {
    // empty
  }

  private void attachAuthToken(OAuthBearerTokenCallback callback) {
    if (callback.token() != null) {
      throw new IllegalArgumentException("Callback had a token already");
    }

    // Create a useless OAuth token. Since we don't use OAuth for
    // inter-broker communication, this won't see use at all but needs to be done due to KAFKA-7462
    callback.token(new OAuthBearerJwsToken("", Collections.emptySet(), -1,
            "", -1L, "", Collections.emptyList()));
  }
}
