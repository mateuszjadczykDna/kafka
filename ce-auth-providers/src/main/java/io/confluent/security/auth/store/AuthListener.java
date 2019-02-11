// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store;

import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.UserMetadata;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public interface AuthListener {

  void onRoleAssignmentAdd(RoleAssignment assignment);

  void onRoleAssignmentDelete(RoleAssignment assignment);

  void onUserUpdate(KafkaPrincipal userPrincipal, UserMetadata userMetadata);

  void onUserDelete(KafkaPrincipal userPrincipal);


}