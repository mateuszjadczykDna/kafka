// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class MultiTenantPrincipal extends KafkaPrincipal {

  private final TenantMetadata tenantMetadata;

  public MultiTenantPrincipal(String username, TenantMetadata tenantMetadata) {
    super(KafkaPrincipal.USER_TYPE, username);

    this.tenantMetadata = tenantMetadata;
  }

  public TenantMetadata tenantMetadata() {
    return tenantMetadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    MultiTenantPrincipal that = (MultiTenantPrincipal) o;
    return tenantMetadata != null ? tenantMetadata.equals(that.tenantMetadata) :
        that.tenantMetadata == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (tenantMetadata != null ? tenantMetadata.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MultiTenantPrincipal("
        + "tenantMetadata=" + tenantMetadata + ", "
        + "user=" + getName() + ")";
  }

}
