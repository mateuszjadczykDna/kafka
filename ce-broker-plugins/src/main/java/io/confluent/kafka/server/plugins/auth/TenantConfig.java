// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import com.google.gson.annotations.SerializedName;
import java.util.Map;
import java.util.Objects;

class TenantConfig {

  @SerializedName("keys") final Map<String, KeyConfigEntry> apiKeys;
  @SerializedName("quotas") final Map<String, QuotaConfigEntry> quotas;

  TenantConfig(Map<String, KeyConfigEntry> apiKeys, Map<String, QuotaConfigEntry> quotas) {
    this.apiKeys = apiKeys;
    this.quotas = quotas;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TenantConfig that = (TenantConfig) o;
    return Objects.equals(apiKeys, that.apiKeys) && Objects.equals(quotas, that.quotas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiKeys, quotas);
  }
}
