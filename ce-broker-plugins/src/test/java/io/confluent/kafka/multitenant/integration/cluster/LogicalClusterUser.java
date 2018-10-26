// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.integration.cluster;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.ScramLoginModule;

/**
 * LogicalClusterUser encapsulates all the metadata associated with a user
 * in a multi-tenant {@link PhysicalCluster}. Each LogicalClusterUser belongs to
 * a single {@link LogicalCluster}.
 */
public class LogicalClusterUser {
  public final UserMetadata userMetadata;
  public final String logicalClusterId;

  public LogicalClusterUser(UserMetadata userMetadata, String logicalClusterId) {
    this.userMetadata = userMetadata;
    this.logicalClusterId = logicalClusterId;
  }

  public String saslUserName() {
    return withPrefix(userMetadata.apiKey());
  }

  public String saslJaasConfig() {
    return String.format("%s required username=\"%s\" password=\"%s\";",
        ScramLoginModule.class.getName(),
        saslUserName(), userMetadata.apiSecret());
  }

  public KafkaPrincipal prefixedKafkaPrincipal() {
    return new KafkaPrincipal(MultiTenantPrincipal.TENANT_USER_TYPE,
        withPrefix(String.valueOf(userMetadata.userId())));
  }

  public KafkaPrincipal unprefixedKafkaPrincipal() {
    return new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
        String.valueOf(userMetadata.userId()));
  }

  public String tenantPrefix() {
    return logicalClusterId + "_";
  }

  public String withPrefix(String name) {
    return tenantPrefix() + name;
  }

  @Override
  public String toString() {
    return "LogicalClusterUser: " + saslUserName();
  }
}
