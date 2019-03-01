// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertNotNull;

import io.confluent.kafka.security.authorizer.Authorizer;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.kafka.KafkaAuthWriter;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.ClusterAction$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestUtils;

public class RbacClusters {

  private final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private final String kafkaSaslMechanism = "SCRAM-SHA-256";
  private final EmbeddedKafkaCluster metadataCluster;
  public final EmbeddedKafkaCluster kafkaCluster;
  private final Map<String, User> users;

  public RbacClusters(String authorizerScope,
                      String metadataServiceScope,
                      String brokerUser,
                      List<String> userNames) throws Exception {
    RbacMetadataServer.reset();

    metadataCluster = new EmbeddedKafkaCluster();
    metadataCluster.startZooKeeper();
    metadataCluster.startBrokers(1, metadataClusterServerConfig());

    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();

    users = createUsers(brokerUser, userNames);
    kafkaCluster.startBrokers(1, serverConfig(brokerUser, authorizerScope, metadataServiceScope));

    TestUtils.waitForCondition(() -> RbacMetadataServer.instance != null, "Metadata server not created");
    TestUtils.waitForCondition(() -> RbacMetadataServer.instance.authStore != null,
        30000, "Metadata server not started");
    assertNotNull(RbacMetadataServer.instance.authStore.writer());
  }

  public void produceConsume(String user,
      String topic,
      String consumerGroup,
      boolean authorized) throws Throwable {
    ClientBuilder clientBuilder = clientBuilder(user);
    KafkaTestUtils.verifyProduceConsume(clientBuilder, topic, consumerGroup, authorized);
  }

  public ClientBuilder clientBuilder(String user) {
    return new ClientBuilder(kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig);
  }

  public void assignRole(String principalType,
                         String userName,
                         String role,
                         String scope,
                         Set<Resource> resources) {
    KafkaPrincipal principal = new KafkaPrincipal(principalType, userName);
    RbacMetadataServer.writer().setRoleResources(
        principal,
        role,
        scope,
        resources);
  }

  public void updateUserGroups(String userName, String... groups) {
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);
    Set<KafkaPrincipal> groupPrincipals = Arrays.stream(groups)
        .map(group -> new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, group))
        .collect(Collectors.toSet());
    UserKey key = new UserKey(principal);
    UserValue value = new UserValue(groupPrincipals);
    RbacMetadataServer.writer().write(key, value, null);
  }

  public void shutdown() {
    try {
      kafkaCluster.shutdown();
    } finally {
      metadataCluster.shutdown();
    }
  }

  public void waitUntilAccessAllowed(String user, String topic) throws Exception {
    try (KafkaProducer<String, String> producer = KafkaTestUtils.createProducer(
        kafkaCluster.bootstrapServers(),
        kafkaSecurityProtocol,
        kafkaSaslMechanism,
        users.get(user).jaasConfig)) {
      TestUtils.waitForCondition(() -> KafkaTestUtils.canAccess(producer, topic), "Access not granted in time");
    }
  }

  private Properties serverConfig(String brokerUser,
                                  String authorizerScope,
                                  String metadataServiceScope) {
    Properties serverConfig = new Properties();
    serverConfig.setProperty(KafkaConfig$.MODULE$.ListenersProp(),
        kafkaSecurityProtocol + "://localhost:0");
    serverConfig.setProperty(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
        kafkaSecurityProtocol.name);
    serverConfig.setProperty(KafkaConfig$.MODULE$.SaslEnabledMechanismsProp(),
        kafkaSaslMechanism);
    serverConfig.setProperty(KafkaConfig$.MODULE$.SaslMechanismInterBrokerProtocolProp(),
        kafkaSaslMechanism);
    String brokerJaasConfigProp = ListenerName.forSecurityProtocol(kafkaSecurityProtocol)
        .saslMechanismConfigPrefix(kafkaSaslMechanism) + KafkaConfig$.MODULE$.SaslJaasConfigProp();
    serverConfig.setProperty(brokerJaasConfigProp, users.get(brokerUser).jaasConfig);

    serverConfig.setProperty(KafkaConfig$.MODULE$.AuthorizerClassNameProp(),
        ConfluentKafkaAuthorizer.class.getName());
    serverConfig.setProperty("super.users", "User:" + brokerUser);
    serverConfig.setProperty(ConfluentAuthorizerConfig.SCOPE_PROP, authorizerScope);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL,RBAC");
    serverConfig.setProperty(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, "RBAC");
    serverConfig.setProperty(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP, "RBAC");
    serverConfig.setProperty(MetadataServiceConfig.SCOPE_PROP, metadataServiceScope);
    serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_LISTENERS_PROP, "http://0.0.0.0:8000");
    serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_ADVERTISED_LISTENERS_PROP, "http://localhost:8000");
    serverConfig.setProperty(MetadataServiceConfig.METADATA_SERVER_CLASS_PROP,
        RbacMetadataServer.class.getName());
    serverConfig.setProperty(KafkaStoreConfig.PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        metadataCluster.bootstrapServers());

    return serverConfig;
  }

  private Properties metadataClusterServerConfig() {
    Properties serverConfig = new Properties();
    serverConfig.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), "100");
    serverConfig.setProperty(KafkaConfig$.MODULE$.ListenersProp(), "PLAINTEXT://localhost:0");
    return serverConfig;
  }

  private Map<String, User> createUsers(String brokerUser, List<String> otherUsers) {
    Map<String, User> users = new HashMap<>(otherUsers.size() + 1);
    users.put(brokerUser, User.createScramUser(kafkaCluster, brokerUser));
    otherUsers.stream()
        .map(name -> User.createScramUser(kafkaCluster, name))
        .forEach(user -> users.put(user.name, user));

    String zkConnect = kafkaCluster.zkConnect();
    KafkaPrincipal brokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, brokerUser);
    AclCommand.main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, ClusterAction$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.clusterAclArgs(zkConnect, brokerPrincipal, Alter$.MODULE$.name()));
    AclCommand.main(SecurityTestUtils.topicBrokerReadAclArgs(zkConnect, brokerPrincipal));
    return users;
  }


  public static class RbacMetadataServer implements MetadataServer {
    volatile static RbacMetadataServer instance;
    volatile Authorizer embeddedAuthorizer;
    volatile AuthStore authStore;

    public RbacMetadataServer() {
      if (instance != null)
        throw new IllegalStateException("Too many metadata servers");
      instance = this;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void start(Authorizer embeddedAuthorizer, AuthStore authStore) {
      this.embeddedAuthorizer = embeddedAuthorizer;
      this.authStore = authStore;
    }

    @Override
    public void close() throws IOException {
      reset();
    }

    static KafkaAuthWriter writer() {
      return (KafkaAuthWriter) instance.authStore.writer();
    }

    static void reset() {
      instance = null;
    }
  }
}
