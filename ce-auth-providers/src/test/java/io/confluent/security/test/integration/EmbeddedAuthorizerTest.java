// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.Scope;
import io.confluent.security.test.integration.rbac.RbacEndToEndAuthorizationTest;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Before;

public class EmbeddedAuthorizerTest extends RbacEndToEndAuthorizationTest {

  private static final Scope SCOPE = new Scope("confluent");

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    KafkaTestUtils.setFinalField(rbacCluster.authCache, AuthCache.class, "rootScope", SCOPE);
    createAdditionalRoles("confluent/core/anotherCluster");
    createAdditionalRoles("confluent/anotherDepartment/testCluster");
  }

  private void createAdditionalRoles(String cluster) throws Exception {
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "Developer", cluster,
        new RbacResource("Topic", APP1_TOPIC, PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "Developer", cluster,
        new RbacResource("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "Developer", cluster,
        new RbacResource("Topic", "app2", PatternType.PREFIXED));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "Developer", cluster,
        new RbacResource("Group", "app2", PatternType.PREFIXED));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "Resource Owner", cluster,
        new RbacResource("Topic", "*", PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "Resource Owner", cluster,
        new RbacResource("Group", "*", PatternType.LITERAL));
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", cluster, null);
    rbacCluster.assignRole(KafkaPrincipal.USER_TYPE, SUPER_USER, "Super User", cluster, null);
  }
}

