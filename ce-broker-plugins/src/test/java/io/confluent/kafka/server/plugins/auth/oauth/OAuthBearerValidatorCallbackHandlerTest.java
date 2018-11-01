// (Copyright) [2017 - 2018] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.oauth;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.junit.Test;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


public class OAuthBearerValidatorCallbackHandlerTest {
  private OAuthUtils.JwsContainer jwsContainer;
  private String defaultIssuer = "Confluent";
  private String defaultSubject = "Lyft <3";
  private String[] defaultAllowedClusters = new String[] {"cluster1"};

  @Test
  public void testAttachesJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(
            baseOptions());

    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(jwsContainer.getJwsToken());
    callbackHandler.handle(new Callback[]{callback});

    assertNotNull(callback.token());
    assertEquals(jwsContainer.getJwsToken(), callback.token().value());
    assertNull(callback.errorStatus());
  }

  @Test(expected = ConfigException.class)
  public void testConfigureRaisesJwtExceptionWhenInvalidKeyPath() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    Map<String, String> options = baseOptions();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath() + "/invalid!");

    createCallbackHandler(options);
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenInvalidJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, defaultSubject, defaultAllowedClusters);
    // override file with new public key
    OAuthUtils.writePemFile(jwsContainer.getPublicKeyFile(), OAuthUtils.generateKeyPair().getPublic());
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionWhenExpiredJws() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(50, defaultIssuer, defaultSubject, defaultAllowedClusters);
    Thread.sleep(100);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfDifferentIssuer() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, "AWS", defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfMissingSubject() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(36000, defaultIssuer, null, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }

  @Test(expected = OAuthBearerValidatorCallbackHandler.JwtVerificationException.class)
  public void testRaisesJwtExceptionIfNoExpirationTime() throws Exception {
    jwsContainer = OAuthUtils.setUpJws(null, defaultIssuer, defaultSubject, defaultAllowedClusters);
    OAuthBearerValidatorCallbackHandler callbackHandler = createCallbackHandler(baseOptions());

    callbackHandler.processToken(jwsContainer.getJwsToken());
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private static OAuthBearerValidatorCallbackHandler createCallbackHandler(Map<String, String> options) {
    TestJaasConfig config = new TestJaasConfig();
    config.createOrUpdateEntry("Kafka", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
            (Map) options);

    OAuthBearerValidatorCallbackHandler callbackHandler = new OAuthBearerValidatorCallbackHandler();
    callbackHandler.configure(Collections.emptyMap(),
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            Collections.singletonList(config.getAppConfigurationEntry("Kafka")[0]));
    return callbackHandler;
  }

  private Map<String, String> baseOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("publicKeyPath", jwsContainer.getPublicKeyFile().getAbsolutePath());
    options.put("audience", String.join(","));
    return options;
  }
}
