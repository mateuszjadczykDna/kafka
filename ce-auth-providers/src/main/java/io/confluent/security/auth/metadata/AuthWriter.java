// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.kafka.security.authorizer.Resource;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Writer interface used by Metadata Server to update role assignments. All update methods are
 * asynchronous and the returned future completes when the update has been written to
 * log, acknowledged and has been consumed by the local reader. Update methods may block for
 * writer to be ready if a rebalance is in progress. Incremental update methods will also block
 * until local cache is up-to-date.
 */
public interface AuthWriter {

  /**
   * Adds a new role assignment without any resources. If the specified role has resource-level
   * scope, no access rules are added for the principal until resources are added to the role
   * using {@link #addRoleResources(KafkaPrincipal, String, String, Collection)}.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> addRoleAssignment(KafkaPrincipal principal, String role, String scope);

  /**
   * Adds resources to a role assignment. If the role is not assigned to the principal, an
   * assignment will be added with the specified resources. If an assignment exists, the provided
   * roles will be added to the list of resources. This method will block until the local cache is
   * up-to-date and the new assignment is queued for update with the updated resources.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Resources to add to role assignment
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> addRoleResources(KafkaPrincipal principal, String role, String scope, Collection<Resource> resources);

  /**
   * Removes a role assignment. If the specified role has resource-level scope, role
   * assignment is removed for all assigned resources.
   *
   * @param principal User or group principal from which role is removed
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> removeRoleAssignment(KafkaPrincipal principal, String role, String scope);

  /**
   * Removes resources from an existing role assignment. This method will block until the
   * local cache is up-to-date and a new assignment is queued with the updated resources.
   *
   * @param principal User or group principal from which role is removed
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Resources being removed for the role assignment
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> removeRoleResources(KafkaPrincipal principal, String role, String scope, Collection<Resource> resources);

  /**
   * Sets resources for an existing role assignment. If the role doesn't exist, a new role
   * is created with the provided set of resources.
   *
   * @param principal User or group principal to which role is assigned
   * @param role Name of role
   * @param scope Scope at which role is assigned
   * @param resources Updated collection of resources for the role assignment
   * @return a stage that is completed when update completes
   */
  CompletionStage<Void> setRoleResources(KafkaPrincipal principal, String role, String scope, Collection<Resource> resources);

}
