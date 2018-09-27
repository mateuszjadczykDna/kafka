// (Copyright) [2017 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.oauth.OAuthBearerJwsToken;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;

/**
 * Principal builder that returns a {@link MultiTenantPrincipal} if tenant
 * id is available. A regular {@link KafkaPrincipal} without tenant
 * information is returned otherwise.
 */
public class MultiTenantPrincipalBuilder implements KafkaPrincipalBuilder {
  private static final Logger log = LoggerFactory.getLogger(MultiTenantPrincipalBuilder.class);

  private static final String OAUTH_NEGOTIATED_TOKEN_PROPERTY_KEY =
          OAuthBearerLoginModule.OAUTHBEARER_MECHANISM + ".token";
  public static final String OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY = "logicalCluster";

  @Override
  public KafkaPrincipal build(AuthenticationContext context) {
    if (context instanceof SaslAuthenticationContext) {
      SaslServer saslServer = ((SaslAuthenticationContext) context).server();
      String authId = saslServer.getAuthorizationID();
      if (saslServer instanceof MultiTenantSaslServer) {
        return new MultiTenantPrincipal(authId,
                ((MultiTenantSaslServer) saslServer).tenantMetadata());
      } else if (saslServer instanceof OAuthBearerSaslServer) {
        OAuthBearerSaslServer server = (OAuthBearerSaslServer) saslServer;
        OAuthBearerJwsToken token = (OAuthBearerJwsToken) server.getNegotiatedProperty(
                OAUTH_NEGOTIATED_TOKEN_PROPERTY_KEY);
        String logicalCluster = (String) server.getNegotiatedProperty(
                OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY);

        return new MultiTenantPrincipal(
                token.principalName(),
                new TenantMetadata(logicalCluster, logicalCluster)
        );
      } else {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, authId);
      }
    } else if (context instanceof SslAuthenticationContext) {
      SSLSession sslSession = ((SslAuthenticationContext) context).session();
      try {
        // For SSL, tenant id may be specified in the certificate as one of the fields
        // of the distinguished name. The code below may be updated to retrieve tenant
        // from the principal to support multi-tenant SSL.
        Principal sslPrincipal = sslSession.getPeerPrincipal();
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, sslPrincipal.getName());
      } catch (SSLPeerUnverifiedException se) {
        return KafkaPrincipal.ANONYMOUS;
      }
    } else if (context instanceof PlaintextAuthenticationContext) {
      // For PLAINTEXT, tenant id may be derived from client addresses. The code
      // below may be updated to assign tenant to PLAINTEXT clients.
      return KafkaPrincipal.ANONYMOUS;
    } else {
      throw new IllegalArgumentException("Unhandled authentication context type: "
        + context.getClass().getName());
    }
  }
}
