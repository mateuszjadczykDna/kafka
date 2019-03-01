// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.security.rbac.utils.JsonMapper;
import io.confluent.security.test.utils.JsonTestUtils;
import java.util.Collection;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RbacAssignmentTest {

  @Test
  public void testAssignment() throws Exception {
    RoleAssignment alice = roleAssignment(
        "{ \"principal\": \"User:Alice\", \"role\": \"Cluster Admin\", \"scope\": \"ClusterA\" }");
    assertEquals(new KafkaPrincipal("User", "Alice"), alice.principal());
    assertEquals("Cluster Admin", alice.role());
    assertEquals("ClusterA", alice.scope());
    assertTrue(alice.resources().isEmpty());
    verifyEquals(alice, roleAssignment(JsonMapper.objectMapper().writeValueAsString(alice)));

    RoleAssignment bob = roleAssignment(
        "{ \"principal\": \"User:Bob\", \"role\": \"Developer\", \"scope\": \"ClusterB\", " +
            "\"resources\" : [ {\"resourceType\": \"Topic\", \"patternType\": \"PREFIXED\", \"name\": \"Finance\"}," +
            "{\"resourceType\": \"Group\", \"patternType\": \"LITERAL\", \"name\": \"*\"}," +
            "{\"resourceType\": \"App\", \"patternType\": \"LITERAL\", \"name\": \"FinanceAppA\"} ] }");
    assertEquals(new KafkaPrincipal("User", "Bob"), bob.principal());
    assertEquals("Developer", bob.role());
    assertEquals("ClusterB", bob.scope());
    Collection<Resource> resources = bob.resources();
    assertEquals(3, resources.size());
    verifyEquals(bob, roleAssignment(JsonMapper.objectMapper().writeValueAsString(bob)));
  }

  private RoleAssignment roleAssignment(String json) {
    return JsonTestUtils.jsonObject(RoleAssignment.class, json);
  }

  private void verifyEquals(RoleAssignment assignment1, RoleAssignment assignment2) {
    assertEquals(assignment1.principal(), assignment2.principal());
    assertEquals(assignment1.role(), assignment2.role());
    assertEquals(assignment1.scope(), assignment2.scope());
    assertEquals(assignment1.resources(), assignment2.resources());
    assertEquals(assignment1, assignment2);
  }
}