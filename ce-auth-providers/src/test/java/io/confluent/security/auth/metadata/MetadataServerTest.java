// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.Action;
import io.confluent.kafka.security.authorizer.AuthorizeResult;
import io.confluent.kafka.security.authorizer.Authorizer;
import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.EmbeddedAuthorizer;
import io.confluent.kafka.security.authorizer.Operation;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.ResourceType;
import io.confluent.security.auth.metadata.MetadataServerTest.TestMetadataServer.ServerState;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.KafkaAuthCache;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.Scope;
import io.confluent.security.test.utils.RbacTestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataServerTest {

  private final String clusterA = "testOrg/clusterA";
  private EmbeddedAuthorizer authorizer;
  private RbacProvider metadataRbacProvider;
  private RbacResource topic = new RbacResource("Topic", "topicA", PatternType.LITERAL);

  @Before
  public void setUp() throws Exception {
    TestMetadataServer.reset();
  }

  @After
  public void tearDown() {
    TestMetadataServer.reset();
    if (authorizer != null)
      authorizer.close();
  }

  @Test
  public void testMetadataServer() {
    createEmbeddedAuthorizer(Collections.emptyMap());
    TestMetadataServer metadataServer = TestMetadataServer.instance;
    assertEquals(ServerState.STARTED, metadataServer.serverState);
    assertNotNull(metadataServer.authStore);
    assertNotNull(metadataServer.embeddedAuthorizer);
    assertTrue(metadataServer.embeddedAuthorizer instanceof EmbeddedAuthorizer);
    EmbeddedAuthorizer authorizer = (EmbeddedAuthorizer) metadataServer.embeddedAuthorizer;
    metadataRbacProvider = (RbacProvider) authorizer.accessRuleProvider("RBAC");
    assertNotNull(metadataRbacProvider);
    KafkaAuthCache metadataAuthCache = metadataRbacProvider.authStore().authCache();
    assertSame(metadataServer.authStore.authCache(), metadataAuthCache);
    assertEquals(new Scope("testOrg"), metadataAuthCache.rootScope());

    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    Set<KafkaPrincipal> groups = Collections.emptySet();

    RbacTestUtils.addRoleAssignment(metadataAuthCache, alice, "Cluster Admin", clusterA, null);
    verifyRules(accessRules(alice, groups, Resource.CLUSTER), "Alter", "Describe", "AlterConfigs", "DescribeConfigs");
    verifyRules(accessRules(alice, groups, topic));

    Action alterConfigs = new Action(clusterA, ResourceType.CLUSTER, "kafka-cluster", new Operation("AlterConfigs"));
    assertEquals(Collections.singletonList(AuthorizeResult.ALLOWED),
        metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(alterConfigs)));

    Action readTopic = new Action(clusterA, new ResourceType("Topic"), "testtopic", new Operation("Read"));
    assertEquals(Collections.singletonList(AuthorizeResult.DENIED),
        metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(readTopic)));

    Action describeAnotherScope = new Action("otherOrg", ResourceType.CLUSTER, "kafka-cluster", new Operation("AlterConfigs"));
    assertEquals(Collections.singletonList(AuthorizeResult.UNKNOWN_SCOPE),
        metadataServer.embeddedAuthorizer.authorize(alice, "localhost", Collections.singletonList(describeAnotherScope)));
  }

  @Test
  public void testMetadataServerConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("confluent.metadata.server.custom.config", "some.value");
    configs.put("confluent.metadata.server.ssl.truststore.location", "trust.jks");
    configs.put("ssl.keystore.location", "key.jks");
    createEmbeddedAuthorizer(configs);
    TestMetadataServer metadataServer = TestMetadataServer.instance;
    assertEquals("some.value", metadataServer.configs.get("confluent.metadata.server.custom.config"));
    assertEquals("trust.jks", metadataServer.configs.get("ssl.truststore.location"));
    assertEquals("key.jks", metadataServer.configs.get("ssl.keystore.location"));
  }

  @Test(expected = ConfigException.class)
  public void testInvalidScope() {
    createEmbeddedAuthorizer(Collections.singletonMap(MetadataServiceConfig.SCOPE_PROP, ""));
  }

  @Test(expected = ConfigException.class)
  public void testMissingMetadataServer() {
    createEmbeddedAuthorizer(Collections.singletonMap(MetadataServiceConfig.METADATA_SERVER_CLASS_PROP, null));
  }

  @Test(expected = ConfigException.class)
  public void testInvalidMetadataServer() {
    createEmbeddedAuthorizer(Collections.singletonMap(MetadataServiceConfig.METADATA_SERVER_CLASS_PROP, "some.class"));
  }

  private void createEmbeddedAuthorizer(Map<String, Object> configOverrides) {
    authorizer = new EmbeddedAuthorizer();
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConfluentAuthorizerConfig.SCOPE_PROP, clusterA);
    configs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "RBAC");
    configs.put(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP, "RBAC");
    configs.put(MetadataServiceConfig.SCOPE_PROP, "testOrg");
    configs.put(MetadataServiceConfig.METADATA_SERVER_CLASS_PROP, TestMetadataServer.class.getName());
    configs.putAll(configOverrides);
    authorizer.configure(configs);
    assertNotNull(TestMetadataServer.instance);
  }

  private Set<AccessRule> accessRules(KafkaPrincipal userPrincipal,
                                      Set<KafkaPrincipal> groupPrincipals,
                                      Resource resource) {
    return metadataRbacProvider.accessRules(userPrincipal, groupPrincipals, clusterA, resource);
  }

  private void verifyRules(Set<AccessRule> rules, String... expectedOps) {
    Set<String> actualOps = rules.stream().map(r -> r.operation().name()).collect(Collectors.toSet());
    assertEquals(Utils.mkSet(expectedOps), actualOps);
  }

  public static class TestMetadataServer implements MetadataServer {
    enum ServerState {
      CREATED,
      CONFIGURED,
      STARTED,
      CLOSED
    }
    volatile static TestMetadataServer instance;
    volatile ServerState serverState;
    volatile Authorizer embeddedAuthorizer;
    volatile AuthStore authStore;
    volatile Map<String, ?> configs;

    public TestMetadataServer() {
      if (instance != null)
        throw new IllegalStateException("Too many metadata servers, state: " + instance.serverState);
      this.serverState = ServerState.CREATED;
      instance = this;
    }

    @Override
    public void configure(Map<String, ?> configs) {
      if (serverState != ServerState.CREATED)
        throw new IllegalStateException("configure() invoked in invalid state: " + serverState);
      this.configs = configs;
      serverState = ServerState.CONFIGURED;
    }

    @Override
    public void start(Authorizer embeddedAuthorizer, AuthStore authStore) {
      if (serverState != ServerState.CONFIGURED)
        throw new IllegalStateException("start() invoked in invalid state: " + serverState);

      this.embeddedAuthorizer = embeddedAuthorizer;
      this.authStore = authStore;
      serverState = ServerState.STARTED;
    }

    @Override
    public void close() throws IOException {
      serverState = ServerState.CLOSED;
    }

    static void reset() {
      instance = null;
    }
  }
}

