// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.rbac;

import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.test.utils.RbacKafkaCluster;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RbacEndToEndAuthorizationTest {

  private static final String CLUSTER = "confluent/core/testCluster";
  private static final String BROKER_USER = "kafka";
  protected static final String SUPER_USER = "root";
  protected static final String DEVELOPER1 = "app1-developer";
  protected static final String DEVELOPER2 = "app2-developer";
  protected static final String RESOURCE_OWNER = "resourceOwner1";
  protected static final String OPERATOR = "operator1";
  private static final String DEVELOPER_GROUP = "app-developers";

  protected static final String APP1_TOPIC = "app1-topic";
  protected static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  private static final String APP2_TOPIC = "app2-topic";
  private static final String APP2_CONSUMER_GROUP = "app2-consumer-group";

  protected RbacKafkaCluster rbacCluster;

  @Before
  public void setUp() throws Throwable {
    List<String> otherUsers = Arrays.asList(
        SUPER_USER,
        DEVELOPER1,
        DEVELOPER2,
        RESOURCE_OWNER,
        OPERATOR
    );
    rbacCluster = new RbacKafkaCluster(CLUSTER, BROKER_USER, otherUsers);

    rbacCluster.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);
    rbacCluster.kafkaCluster.createTopic(APP2_TOPIC, 2, 1);

    initializeRoles();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (rbacCluster != null)
        rbacCluster.kafkaCluster.shutdown();
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
    }
  }

  @Test
  public void testProduceConsumeWithRbac() throws Throwable {
    rbacCluster.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacCluster.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    rbacCluster.produceConsume(DEVELOPER2, APP2_TOPIC, APP2_CONSUMER_GROUP, true);
    rbacCluster.produceConsume(SUPER_USER, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacCluster.produceConsume(SUPER_USER, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacCluster.produceConsume(RESOURCE_OWNER, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacCluster.produceConsume(RESOURCE_OWNER, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testProduceConsumeWithGroupRoles() throws Throwable {
    rbacCluster.updateUserGroups(DEVELOPER2, DEVELOPER_GROUP);
    rbacCluster.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "Developer", CLUSTER,
        new RbacResource("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL));
    rbacCluster.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "Developer", CLUSTER,
        new RbacResource("Topic", APP1_TOPIC, PatternType.LITERAL));
    rbacCluster.waitUntilAccessAllowed(DEVELOPER2, APP1_TOPIC);
    rbacCluster.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  private void initializeRoles() throws Exception {
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "Developer", CLUSTER,
        new RbacResource("Topic", APP1_TOPIC, PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "Developer", CLUSTER,
        new RbacResource("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "Developer", CLUSTER,
        new RbacResource("Topic", "app2", PatternType.PREFIXED));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "Developer", CLUSTER,
        new RbacResource("Group", "app2", PatternType.PREFIXED));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "Resource Owner", CLUSTER,
        new RbacResource("Topic", "*", PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "Resource Owner", CLUSTER,
        new RbacResource("Group", "*", PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", CLUSTER, null);
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, SUPER_USER, "Super User", CLUSTER, null);

    rbacCluster.waitUntilAccessAllowed(SUPER_USER, APP1_TOPIC);
  }
}

