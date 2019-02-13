// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.provider.rbac.RbacProvider;
import io.confluent.security.auth.store.KafkaAuthCache;
import io.confluent.security.rbac.RbacResource;
import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.UserMetadata;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class RbacTestUtils {

  public static void addRoleAssignment(KafkaAuthCache authCache, KafkaPrincipal principal, String role, String scope, RbacResource resource) {
    authCache.onRoleAssignmentAdd(roleAssignment(principal, role, scope, resource));
  }

  public static void deleteRoleAssignment(KafkaAuthCache authCache, KafkaPrincipal principal, String role, String scope, RbacResource resource) {
    authCache.onRoleAssignmentDelete(roleAssignment(principal, role, scope, resource));
  }

  public static RoleAssignment roleAssignment(KafkaPrincipal principal, String role, String scope, RbacResource resource) {
    return new RoleAssignment(
        principal,
        role,
        scope,
        resource == null ? Collections.emptySet() : Collections.singleton(resource));
  }

  public static void updateUserGroups(KafkaAuthCache authCache, KafkaPrincipal user, Set<KafkaPrincipal> groups) {
    UserMetadata userMetadata = new UserMetadata(groups);
    authCache.onUserUpdate(user, userMetadata);
  }

  public static KafkaAuthCache authCache(RbacProvider rbacProvider) {
    return KafkaTestUtils.fieldValue(rbacProvider, RbacProvider.class, "authCache");
  }
}