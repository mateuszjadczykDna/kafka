// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

class KeyConfigEntry {
  @SerializedName("sasl_mechanism") final String saslMechanism;
  @SerializedName("hashed_secret") final String hashedSecret;
  @SerializedName("hash_function") final String hashFunction;
  @SerializedName("user_id") final String userId;
  // logical cluster ID (appears to be it's own cluster from customer's POV)
  @SerializedName("logical_cluster_id") final String logicalClusterId;

  KeyConfigEntry(String saslMechanism, String hashedSecret, String hashFunction, String userId,
      String tenantId, String logicalClusterId) {
    this.saslMechanism = saslMechanism;
    this.hashedSecret = hashedSecret;
    this.hashFunction = hashFunction;
    this.userId = userId;
    this.logicalClusterId = logicalClusterId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyConfigEntry that = (KeyConfigEntry) o;
    return Objects.equals(saslMechanism, that.saslMechanism)
        && Objects.equals(hashedSecret, that.hashedSecret)
        && Objects.equals(hashFunction, that.hashFunction)
        && Objects.equals(userId, that.userId)
        && Objects.equals(logicalClusterId, that.logicalClusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        saslMechanism, hashedSecret, hashFunction, userId, logicalClusterId
    );
  }
}
