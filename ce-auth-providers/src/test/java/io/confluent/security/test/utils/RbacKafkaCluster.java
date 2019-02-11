// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.kafka.security.authorizer.EmbeddedAuthorizer;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.rbac.RbacResource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import kafka.admin.AclCommand;
import kafka.security.auth.Alter$;
import kafka.security.auth.Authorizer;
import kafka.security.auth.ClusterAction$;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.test.TestUtils;
import scala.Option;

public class RbacKafkaCluster {

  private final SecurityProtocol kafkaSecurityProtocol = SecurityProtocol.SASL_PLAINTEXT;
  private final String kafkaSaslMechanism = "SCRAM-SHA-256";
  public final EmbeddedKafkaCluster kafkaCluster;
  private final Map<String, User> users;
  public AuthCache authCache;

  public RbacKafkaCluster(String authorizerScope,
                          String brokerUser,
                          List<String> userNames) throws Exception {
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();

    users = createUsers(brokerUser, userNames);
    kafkaCluster.startBrokers(1, serverConfig(brokerUser, authorizerScope));
    authCache = authCache();
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

  public void assignRole(String principalType, String userName, String role, String scope, RbacResource resource) {
    KafkaPrincipal principal = new KafkaPrincipal(principalType, userName);
    RbacTestUtils.addRoleAssignment(authCache, principal, role, scope, resource);
  }

  public void updateUserGroups(String userName, String... groups) {
    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName);
    Set<KafkaPrincipal> groupPrincipals = Arrays.stream(groups)
        .map(group -> new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, group))
        .collect(Collectors.toSet());
    RbacTestUtils.updateUserGroups(authCache, principal, groupPrincipals);
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

  private Properties serverConfig(String brokerUser, String authorizerScope) {
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
    serverConfig.setProperty(ConfluentAuthorizerConfig.SCOPE_PROP, authorizerScope);
    serverConfig.setProperty(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ACL,RBAC");
    serverConfig.setProperty(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, "RBAC");

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

  // RS: TODO: This is temporary code to enable auth cache to be populated. This will be removed
  // when the Kafka storage layer is implemented for RBAC.
  private AuthCache authCache() {
    assertEquals(1, kafkaCluster.brokers().size());
    KafkaServer server = kafkaCluster.brokers().get(0);
    AuthCache authCache = null;
    Option<Authorizer> authorizer = KafkaTestUtils.fieldValue(server, KafkaServer.class, "authorizer");
    List<AccessRuleProvider> providers = KafkaTestUtils.fieldValue(authorizer.get(),
        EmbeddedAuthorizer.class, "accessRuleProviders");
    authCache = providers.stream()
        .filter(p -> p instanceof RbacProvider)
        .map(p -> RbacTestUtils.authCache((RbacProvider) p))
        .findFirst()
        .orElse(null);
    assertNotNull("AuthCache not found", authCache);
    return authCache;
  }
}