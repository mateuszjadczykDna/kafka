// (Copyright) [2017 - 2018] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.oauth;

import io.confluent.common.security.util.PemUtils;
import io.confluent.common.security.util.CloudUtils;
import io.confluent.kafka.multitenant.oauth.OAuthBearerJwsToken;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PublicKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;

import static io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY;


/**
 * A {@code CallbackHandler} that recognizes
 * {@link OAuthBearerValidatorCallback} and {@link OAuthBearerExtensionsValidatorCallback}
 * for validating a secured OAuth 2 bearer token issued by Confluent and SASL extensions
 * specifying the logical cluster this token is meant for.
 *
 * <p>It verifies the signature of the JWTToken through a public key it reads from a file path,
 * set in the JAAS config
 *
 * <p>This class must be explicitly set via the
 * {@code listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.server.callback.handler.class}
 * broker configuration property.
 */
public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(
          OAuthBearerValidatorCallbackHandler.class);

  private JwtConsumer jwtConsumer;

  static class JwtVerificationException extends Exception {
    JwtVerificationException(String message) {
      super(message);
    }
  }

  private boolean configured = false;

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs, String saslMechanism,
                        List<AppConfigurationEntry> jaasConfigEntries) {
    if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException(
              String.format("Unexpected SASL mechanism: %s", saslMechanism));
    }
    if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null) {
      throw new IllegalArgumentException(
              String.format(
                      "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                      jaasConfigEntries.size()
              )
      );
    }

    Map<String, String> moduleOptions = Collections.unmodifiableMap(
            (Map<String, String>) jaasConfigEntries.get(0).getOptions());

    try {
      String publicKeyPath = moduleOptions.get("publicKeyPath");
      if (publicKeyPath == null) {
        throw new ConfigException("publicKeyPath option must be set in JAAS config!");
      }

      PublicKey publicKey = PemUtils.loadPublicKey(new FileInputStream(publicKeyPath));
      jwtConsumer = CloudUtils.createJwtConsumer(publicKey);
    } catch (IOException e) {
      throw new ConfigException(String.format("Could not load the public key from %s",
              moduleOptions.get("publicKeyPath")), e);
    }

    configured = true;
  }


  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    if (!configured) {
      throw new IllegalStateException("Callback handler not configured");
    }
    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerValidatorCallback) {
        OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
        try {
          handleValidatorCallback(validationCallback);
        } catch (JwtVerificationException e) {
          log.debug("Failed to verify token: {}", e);
          validationCallback.error("invalid_token", null, null);
        }
      } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
        handleExtensionsCallback((OAuthBearerExtensionsValidatorCallback) callback);
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override
  public void close() {
    // empty
  }

  private void handleValidatorCallback(OAuthBearerValidatorCallback callback)
          throws JwtVerificationException {
    String tokenValue = callback.tokenValue();
    if (tokenValue == null) {
      throw new IllegalArgumentException("Callback missing required token value");
    }
    OAuthBearerJwsToken token = processToken(tokenValue);
    callback.token(token);
    log.debug("Successfully validated token");
  }

  /**
   * Validates that the token's signed claim "clusters" contains the cluster
   * in the unsecured SASL extension the client wants to connect to.
   * This is called after the token is validated.
   */
  private void handleExtensionsCallback(OAuthBearerExtensionsValidatorCallback callback) {
    OAuthBearerJwsToken token = (OAuthBearerJwsToken) callback.token();
    String logicalCluster = callback.inputExtensions().map()
            .get(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY);

    if (logicalCluster == null || logicalCluster.isEmpty()) {
      callback.error(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY,
              "The logical cluster extension is missing or is empty");
      return;
    }

    if (!token.allowedClusters().contains(logicalCluster)) {
      String errorMessage = String.format(
              "The principal's (%s) logical cluster %s is not part of the "
                      + "allowed clusters in his token (%s).",
              token.principalName(), logicalCluster, String.join(",", token.allowedClusters())
      );
      callback.error(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, errorMessage);
      return;
    }

    callback.valid(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY);
    log.info("Successfully authenticated for user: {} (cluster: {})",
            token.principalName(), logicalCluster);
  }

  /**
   * Validates the JWS token against the jwtConsumer's requirements (e.g not expired, valid issuer).
   * See {@link CloudUtils#createJwtConsumer(PublicKey)} for the JWS token requirements
   */
  OAuthBearerJwsToken processToken(String jws) throws JwtVerificationException {
    try {
      JwtClaims claims = jwtConsumer.processToClaims(jws);

      List<String> allowedClusters = claims.getStringListClaimValue("clusters");
      if (allowedClusters == null || allowedClusters.isEmpty()) {
        throw new JwtVerificationException("clusters claim must be set and "
                + "must contain at least one cluster!");
      }

      return new OAuthBearerJwsToken(jws,
              new HashSet<>(),
              claims.getExpirationTime().getValueInMillis(),
              claims.getSubject(),
              claims.getIssuedAt().getValueInMillis(),
              claims.getJwtId(),
              allowedClusters);
    } catch (InvalidJwtException | MalformedClaimException e) {
      throw new JwtVerificationException(e.getMessage());
    }
  }
}
