// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import java.util.Objects;

import io.confluent.kafka.multitenant.utils.Utils;

public class TenantMetadata {

  public final String tenantName;
  public final String clusterId;
  public final boolean allowDescribeBrokerConfigs;

  public TenantMetadata(String tenantName, String clusterId) {
    this(tenantName, clusterId, false);
  }

  public TenantMetadata(String tenantName, String clusterId, boolean allowDescribeBrokerConfigs) {
    this.tenantName = Utils.requireNonEmpty(tenantName, "Tenant");
    this.clusterId = Utils.requireNonEmpty(clusterId, "ClusterId");
    this.allowDescribeBrokerConfigs = allowDescribeBrokerConfigs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TenantMetadata that = (TenantMetadata) o;
    return Objects.equals(tenantName, that.tenantName)
        && Objects.equals(clusterId, that.clusterId);

  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, clusterId);
  }
}
