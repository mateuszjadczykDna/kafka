// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.rbac.utils.JsonMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The RBAC policy definition. Roles are currently statically defined
 * in the JSON file `rbac_policy.json`.
 */
public class RbacRoles {

  private static final String DEFAULT_POLICY_FILE = "default_rbac_roles.json";

  private final Map<String, Role> roles;

  @JsonCreator
  public RbacRoles(@JsonProperty("roles") List<Role> roles) {
    this.roles = new HashMap<>();
    roles.forEach(this::addRole);
  }

  public Role role(String roleName) {
    return roles.get(roleName);
  }

  public boolean isSuperUser(String roleName) {
    Role role = role(roleName);
    return role != null && role.accessPolicy().isSuperUser();
  }

  Collection<Role> roles() {
    return roles.values();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RbacRoles)) {
      return false;
    }

    RbacRoles that = (RbacRoles) o;
    return Objects.equals(roles, that.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roles);
  }

  void addRole(Role role) {
    String scope = role.accessPolicy().scope();
    if (AccessPolicy.SCOPES.stream().noneMatch(scope::equalsIgnoreCase)) {
      throw new InvalidRoleDefinitionException(String.format("Unknown scope for role definition %s: %s", role, scope));
    }
    this.roles.put(role.name(), role);
  }

  public static RbacRoles loadDefaultPolicy() throws InvalidRoleDefinitionException {
    return load(RbacRoles.class.getClassLoader(), DEFAULT_POLICY_FILE);
  }

  public static RbacRoles load(ClassLoader classLoader, String policyResourceName)
      throws InvalidRoleDefinitionException {
    try {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(policyResourceName)))) {
        return JsonMapper.objectMapper().readValue(reader, RbacRoles.class);
      }
    } catch (IOException e) {
      throw new InvalidRoleDefinitionException("RBAC policies could not be loaded from " + policyResourceName, e);
    }
  }
}
