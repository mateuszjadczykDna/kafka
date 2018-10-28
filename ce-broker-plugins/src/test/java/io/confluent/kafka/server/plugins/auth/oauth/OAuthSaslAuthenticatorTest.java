package io.confluent.kafka.server.plugins.auth.oauth;

import io.confluent.kafka.clients.plugins.auth.oauth.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.CertStores;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.NioEchoServer;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkTestUtils;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class OAuthSaslAuthenticatorTest {
  private Selector selector;
  private NioEchoServer server;
  private OAuthUtils.JwsContainer jwsContainer;
  private Map<String, Object> saslClientConfigs;
  private Map<String, Object> saslServerConfigs;
  private String allowedCluster = "audi";
  private String[] allowedClusters = new String[] { allowedCluster };
  private MockTime mockTime;

  private CredentialCache credentialCache;

  @Before
  public void setup() throws Exception {
    LoginManager.closeAll();
    CertStores serverCertStores = new CertStores(true, "localhost");
    CertStores clientCertStores = new CertStores(false, "localhost");
    this.saslServerConfigs = serverCertStores.getTrustingConfig(clientCertStores);
    this.saslClientConfigs = clientCertStores.getTrustingConfig(serverCertStores);
    this.credentialCache = new CredentialCache();
    this.mockTime = new MockTime(1);
  }

  @After
  public void teardown() throws Exception {
    if (this.server != null) {
      this.server.close();
    }

    if (this.selector != null) {
      this.selector.close();
    }

  }

  @Test
  public void testValidTokenAuthorizes() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "Confluent", "Confluent", allowedClusters);
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnection(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(1, 0);
  }

  @Test
  public void testInvalidIssuerFailsAuthorization() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "SomebodyElse", "Confluent", allowedClusters);
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnectionFailure(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(0, 1);
  }

  @Test
  public void testPublicKeyFailsAuthorization() throws Exception {
    String node = "0";
    SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;
    jwsContainer = OAuthUtils.setUpJws(100000, "SomebodyElse", "Confluent", allowedClusters);
    OAuthUtils.writePemFile(jwsContainer.getPublicKeyFile(), OAuthUtils.generateKeyPair().getPublic());
    this.configureMechanisms("OAUTHBEARER", Collections.singletonList("OAUTHBEARER"));
    this.server = this.createEchoServer(securityProtocol);

    this.createAndCheckClientConnectionFailure(securityProtocol, node);
    this.server.verifyAuthenticationMetrics(0, 1);
  }

  private void configureMechanisms(String clientMechanism, List<String> serverMechanisms) {
    this.saslClientConfigs.put("sasl.mechanism", clientMechanism);
    this.saslClientConfigs.put("sasl.login.callback.handler.class",
            OAuthBearerLoginCallbackHandler.class.getName());
    this.saslClientConfigs.put("sasl.jaas.config", "org.apache.kafka.common.security." +
            "oauthbearer.OAuthBearerLoginModule Required token=\"" + jwsContainer.getJwsToken() +
            "\" cluster=\"" + allowedCluster + "\";");

    this.saslServerConfigs.put("sasl.enabled.mechanisms", serverMechanisms);
    this.saslServerConfigs.put("listener.name.sasl_ssl.oauthbearer.sasl.server.callback" +
            ".handler.class", OAuthBearerValidatorCallbackHandler.class.getName());
    this.saslServerConfigs.put("listener.name.sasl_ssl.oauthbearer.sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    "unsecuredLoginStringClaim_sub=\"Confluent\" publicKeyPath=\"" +
                    jwsContainer.getPublicKeyFile().toPath() +  "\";");

    TestJaasConfig.createConfiguration(clientMechanism, serverMechanisms);
  }

  private void createAndCheckClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
    this.createClientConnection(securityProtocol, node);
    NetworkTestUtils.checkClientConnection(this.selector, node, 100, 10);
    this.selector.close();
    this.selector = null;
  }

  private void createAndCheckClientConnectionFailure(SecurityProtocol securityProtocol, String node)
          throws Exception {
    createClientConnection(securityProtocol, node);
    NetworkTestUtils.waitForChannelClose(selector, node, ChannelState.State.AUTHENTICATION_FAILED);
    selector.close();
    selector = null;
  }

  private void createClientConnection(SecurityProtocol securityProtocol, String node) throws Exception {
    this.createSelector(securityProtocol, this.saslClientConfigs);
    InetSocketAddress addr = new InetSocketAddress("localhost", this.server.port());
    this.selector.connect(node, addr, 4096, 4096);
  }

  private void createSelector(SecurityProtocol securityProtocol, Map<String, Object> clientConfigs) {
    if (this.selector != null) {
      this.selector.close();
      this.selector = null;
    }

    String saslMechanism = (String) this.saslClientConfigs.get("sasl.mechanism");
    ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT,
            new TestSecurityConfig(clientConfigs), (ListenerName) null, saslMechanism, mockTime, true);
    // Create the selector manually instead of using NetworkTestUtils so we can use a longer timeout
    this.selector = new Selector(25000L, new Metrics(), this.mockTime, "MetricGroup",
    channelBuilder, new LogContext());

  }

  private NioEchoServer createEchoServer(SecurityProtocol securityProtocol) throws Exception {
    return NetworkTestUtils.createEchoServer(
            ListenerName.forSecurityProtocol(securityProtocol), securityProtocol,
            new TestSecurityConfig(this.saslServerConfigs), this.credentialCache, this.mockTime
    );
  }
}
