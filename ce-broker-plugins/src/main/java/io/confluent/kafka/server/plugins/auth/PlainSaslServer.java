package io.confluent.kafka.server.plugins.auth;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.MultiTenantSaslServer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * rhoover - split out credential check, added stats and logging
 */

import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.server.plugins.auth.stats.AuthenticationStats;
import io.confluent.kafka.server.plugins.auth.stats.TenantAuthenticationStats;

import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SaslServer implementation for SASL/PLAIN with an authenticator
 * provided through the constructor.
 */
public class PlainSaslServer implements MultiTenantSaslServer {

  public static final String PLAIN_MECHANISM = "PLAIN";
  private final SaslAuthenticator authenticator;
  private static final AuthenticationStats STATS = AuthenticationStats.getInstance();
  private static final TenantAuthenticationStats TENANT_STATS =
      TenantAuthenticationStats.instance();
  private static final Logger log =
          LoggerFactory.getLogger(PlainSaslServer.class);

  private boolean complete;
  private String authorizationID;
  private TenantMetadata tenantMetadata;

  public PlainSaslServer(List<AppConfigurationEntry> jaasContextEntries,
                         SaslAuthenticator authenticator) {
    this.authenticator = authenticator;
    authenticator.initialize(jaasContextEntries);
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    try {
      byte[] result = doEvaluateResponse(response);
      STATS.incrSucceeded();
      return result;
    } catch (Exception e) {
      STATS.incrFailed();
      String cause = e.getCause() == null ? "" : e.getCause().getMessage();
      log.debug("SASL/PLAIN authentication failed: {}", cause, e);
      throw e;
    } finally {
      clearMdc();
    }
  }

  private void clearMdc() {
    MDC.remove("username");
    MDC.remove("saslMechanism");
    MDC.remove("authorizationId");
    MDC.remove("tenant");
  }

  private byte[] doEvaluateResponse(byte[] response) throws SaslException {
    /*
     * Message format (from https://tools.ietf.org/html/rfc4616):
     *
     * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
     * authcid   = 1*SAFE ; MUST accept up to 255 octets
     * authzid   = 1*SAFE ; MUST accept up to 255 octets
     * passwd    = 1*SAFE ; MUST accept up to 255 octets
     * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
     *
     * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
     *                ;; any UTF-8 encoded Unicode character except NUL
     */

    MDC.put("saslMechanism", "PLAIN");

    String[] tokens;
    try {
      tokens = new String(response, "UTF-8").split("\u0000");
    } catch (UnsupportedEncodingException e) {
      throw new SaslException("UTF-8 encoding not supported", e);
    }
    if (tokens.length != 3) {
      throw new SaslException("Invalid SASL/PLAIN response: expected 3 tokens, got "
        + tokens.length);
    }
    final String authorizationIdFromClient = tokens[0];
    final String username = tokens[1];
    final String password = tokens[2];

    if (username.isEmpty()) {
      throw new SaslException("Authentication failed: username not specified");
    }
    MDC.put("username", username);

    if (password.isEmpty()) {
      throw new SaslException("Authentication failed: password not specified");
    }
    if (!authorizationIdFromClient.isEmpty() && !authorizationIdFromClient.equals(username)) {
      throw new SaslException("Authentication failed: Impersonation is not allowed; "
              + "authorization id must match username");
    }

    MultiTenantPrincipal principal = authenticator.authenticate(username, password);
    authorizationID = principal.getName();
    MDC.put("authorizationId", authorizationID);
    tenantMetadata = principal.tenantMetadata();
    MDC.put("tenant", tenantMetadata.tenantName);
    TENANT_STATS.onSuccessfulAuthentication(principal);

    log.debug("SASL/PLAIN authentication succeeded for user {}", username);
    complete = true;
    return new byte[0];
  }

  @Override
  public TenantMetadata tenantMetadata() {
    throwIfNotComplete();
    return tenantMetadata;
  }

  @Override
  public String getAuthorizationID() {
    throwIfNotComplete();
    return authorizationID;
  }

  @Override
  public String getMechanismName() {
    return PLAIN_MECHANISM;
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    throwIfNotComplete();
    return null;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    throwIfNotComplete();
    return Arrays.copyOfRange(incoming, offset, offset + len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    throwIfNotComplete();
    return Arrays.copyOfRange(outgoing, offset, offset + len);
  }

  @Override
  public void dispose() throws SaslException {
  }

  private void throwIfNotComplete() {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
  }

  public static class PlainSaslServerFactory implements SaslServerFactory {
    private final SaslServerSupplier saslServerSupplier;

    public PlainSaslServerFactory(SaslServerSupplier saslServerSupplier) {
      this.saslServerSupplier = saslServerSupplier;
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
            Map<String, ?> props, CallbackHandler cbh) throws SaslException {

      if (!PLAIN_MECHANISM.equals(mechanism)) {
        throw new SaslException(String.format("Mechanism \'%s\' is not supported. "
                + "Only PLAIN is supported.", mechanism));
      }

      if (!(cbh instanceof PlainServerCallbackHandler)) {
        throw new SaslException("CallbackHandler must be of type PlainServerCallbackHandler, "
                + "but it is: " + cbh.getClass());
      }

      // Note: This is a temporary fix to obtain JAAS configuration entries in PlainSaslServer
      // This should be replaced with a custom callback handler that is configured by Kafka with
      // the JAAS configuration entries.
      List<AppConfigurationEntry> jaasContextEntries;
      try {
        Field field = PlainServerCallbackHandler.class.getDeclaredField("jaasConfigEntries");
        field.setAccessible(true);
        jaasContextEntries = (List<AppConfigurationEntry>) field.get(cbh);
      } catch (Throwable e) {
        throw new SaslException("Could not obtain JAAS context", e);
      }
      return saslServerSupplier.get(jaasContextEntries);
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
      if ("true".equals(noPlainText)) {
        return new String[]{};
      } else {
        return new String[]{PLAIN_MECHANISM};
      }
    }
  }
}
