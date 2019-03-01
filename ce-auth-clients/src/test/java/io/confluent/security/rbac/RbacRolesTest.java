// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import io.confluent.kafka.security.authorizer.Operation;
import io.confluent.kafka.security.authorizer.ResourceType;
import io.confluent.security.rbac.utils.JsonMapper;
import io.confluent.security.test.utils.JsonTestUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RbacRolesTest {

  private static final String ADMIN_ROLE = "{ \"name\" : \"admin\" , \"policy\" : " +
      "{ \"scope\" : \"Cluster\", \"allowedOperations\" : [{ \"resourceType\" : \"Topic\", \"operations\" : [\"All\"]}] }}";
  private static final String DEVELOPER_ROLE = "{ \"name\" : \"developer\" , \"policy\" : " +
      "{ \"scope\" : \"Resource\", \"allowedOperations\" : [{ \"resourceType\" : \"Metrics\", \"operations\" : [\"Monitor\"]}] }}";

  private RbacRoles rbacRoles = new RbacRoles(Collections.emptyList());

  @Test
  public void testRoleDefinitions() throws Exception {
    addRoles(ADMIN_ROLE);
    assertEquals(1, rbacRoles.roles().size());
    Role role = rbacRoles.roles().iterator().next();
    assertEquals("admin", role.name());
    AccessPolicy accessPolicy = role.accessPolicy();
    assertNotNull(accessPolicy);
    assertEquals("Cluster", accessPolicy.scope());
    verifyAccessPolicy(accessPolicy, "Topic", "All");

    addRoles(DEVELOPER_ROLE);
    assertEquals(2, rbacRoles.roles().size());
    Role role2 = rbacRoles.role("developer");
    AccessPolicy accessPolicy2 = role2.accessPolicy();
    assertNotNull(accessPolicy2);
    assertEquals("Resource", accessPolicy2.scope());
    verifyAccessPolicy(accessPolicy2, "Metrics", "Monitor");

    assertEquals(accessPolicy, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy)));
    assertEquals(accessPolicy2, accessPolicy(JsonMapper.objectMapper().writeValueAsString(accessPolicy2)));
  }

  @Test(expected = InvalidRoleDefinitionException.class)
  public void testRoleWithUnknownScope() throws Exception {
    String json = "{ \"name\" : \"admin\" , \"policy\" : " +
        "{ \"scope\" : \"unknown\", \"allowedOperations\" : [{ \"resourceType\" : \"All\", \"operations\" : [\"All\"]}] }}";
    addRoles(json);
  }

  @Test
  public void testDefaultRoles() throws Exception {
    RbacRoles rbacRoles = RbacRoles.loadDefaultPolicy();

    assertEquals("Cluster", rbacRoles.role("Super User").accessPolicy().scope());
    assertEquals("Cluster", rbacRoles.role("User Admin").accessPolicy().scope());
    assertEquals("Cluster", rbacRoles.role("Cluster Admin").accessPolicy().scope());
    assertEquals("Cluster", rbacRoles.role("Operator").accessPolicy().scope());
    assertEquals("Cluster", rbacRoles.role("Security Admin").accessPolicy().scope());
    assertEquals("Resource", rbacRoles.role("Resource Owner").accessPolicy().scope());
    assertEquals("Resource", rbacRoles.role("Developer").accessPolicy().scope());

    assertTrue(rbacRoles.role("User Admin").accessPolicy()
        .allowedOperations(new ResourceType("Cluster")).contains(new Operation("Alter")));
    assertTrue(rbacRoles.role("Developer").accessPolicy()
        .allowedOperations(new ResourceType("Group")).contains(new Operation("Read")));
  }

  private void addRoles(String rolesJson) {
    for (Role role : JsonTestUtils.jsonArray(Role[].class, rolesJson)) {
      rbacRoles.addRole(role);
    }
  }

  private void verifyAccessPolicy(AccessPolicy accessPolicy, String expectedResourceType, String... expectedOps) {
    assertEquals(1, accessPolicy.allowedOperations().size());
    assertEquals(expectedResourceType, accessPolicy.allowedOperations().iterator().next().resourceType());
    Collection<Operation> ops = accessPolicy.allowedOperations(new ResourceType(expectedResourceType));
    assertEquals(Utils.mkSet(expectedOps), ops.stream().map(Operation::name).collect(Collectors.toSet()));
  }

  private AccessPolicy accessPolicy(String json) {
    return JsonTestUtils.jsonObject(AccessPolicy.class, json);
  }
}