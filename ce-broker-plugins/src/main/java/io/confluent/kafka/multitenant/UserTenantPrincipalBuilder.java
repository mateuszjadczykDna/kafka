// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;

/**
 * Each user has its own tenant id and namespace. This is used to facilitate simple system
 * testing without the full authorization stack.
 */
public class UserTenantPrincipalBuilder implements KafkaPrincipalBuilder {
  @Override
  public KafkaPrincipal build(AuthenticationContext authenticationContext) {
    String user = KafkaPrincipal.ANONYMOUS.getName();
    if (authenticationContext instanceof SaslAuthenticationContext) {
      user = ((SaslAuthenticationContext) authenticationContext).server().getAuthorizationID();
    }

    TenantMetadata tenantMetadata = new TenantMetadata(user, user);
    return new MultiTenantPrincipal(user, tenantMetadata);
  }
}
