// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;

/**
 * Represents logical cluster metadata
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LogicalClusterMetadata {

  public static final String KAFKA_LOGICAL_CLUSTER_TYPE = "kafka";
  public static final String HEALTHCHECK_LOGICAL_CLUSTER_TYPE = "healthcheck";
  public static final Double DEFAULT_REQUEST_PERCENTAGE =
      50.0 * TenantQuotaCallback.DEFAULT_MIN_PARTITIONS;

  // 100% overhead means that bandwidth quota will be set to byte_rate + 100% of byte_rate
  public static final Integer DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE = 100;
  public static final Long DEFAULT_HEALTHCHECK_MAX_PRODUCER_RATE = 10L * 1024L * 1024L;
  public static final Long DEFAULT_HEALTHCHECK_MAX_CONSUMER_RATE = 10L * 1024L * 1024L;

  private final String logicalClusterId;
  private final String physicalClusterId;
  private final String logicalClusterName;
  private final String accountId;
  private final String k8sClusterId;
  private final String logicalClusterType;
  private final Long storageBytes;
  private final Long producerByteRate;
  private final Long consumerByteRate;
  private final Double requestPercentage;
  private final Integer networkQuotaOverhead;

  @JsonCreator
  public LogicalClusterMetadata(
      @JsonProperty("logical_cluster_id") String logicalClusterId,
      @JsonProperty("physical_cluster_id") String physicalClusterId,
      @JsonProperty("logical_cluster_name") String logicalClusterName,
      @JsonProperty("account_id") String accountId,
      @JsonProperty("k8s_cluster_id") String k8sClusterId,
      @JsonProperty("logical_cluster_type") String logicalClusterType,
      @JsonProperty("storage_bytes") Long storageBytes,
      @JsonProperty("network_ingress_byte_rate") Long producerByteRate,
      @JsonProperty("network_egress_byte_rate") Long consumerByteRate,
      @JsonProperty("request_percentage") Long requestPercentage,
      @JsonProperty("network_quota_overhead") Integer networkQuotaOverhead
  ) {
    this.logicalClusterId = logicalClusterId;
    this.physicalClusterId = physicalClusterId;
    this.logicalClusterName = logicalClusterName;
    this.accountId = accountId;
    this.k8sClusterId = k8sClusterId;
    this.logicalClusterType = logicalClusterType;
    this.storageBytes = storageBytes;
    this.producerByteRate = producerByteRate == null &&
                            HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType) ?
                            DEFAULT_HEALTHCHECK_MAX_PRODUCER_RATE : producerByteRate;
    this.consumerByteRate = consumerByteRate == null &&
                            HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType) ?
                            DEFAULT_HEALTHCHECK_MAX_CONSUMER_RATE : consumerByteRate;
    this.requestPercentage = requestPercentage == null ?
                             DEFAULT_REQUEST_PERCENTAGE : requestPercentage;
    this.networkQuotaOverhead = networkQuotaOverhead == null ?
                                DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE : networkQuotaOverhead;
  }

  @JsonProperty
  public String logicalClusterId() {
    return logicalClusterId;
  }

  @JsonProperty
  public String physicalClusterId() {
    return physicalClusterId;
  }

  @JsonProperty
  public String logicalClusterName() {
    return logicalClusterName;
  }

  @JsonProperty
  public String accountId() {
    return accountId;
  }

  @JsonProperty
  public String k8sClusterId() {
    return k8sClusterId;
  }

  @JsonProperty
  public String logicalClusterType() {
    return logicalClusterType;
  }

  @JsonProperty
  public Long storageBytes() {
    return storageBytes;
  }

  @JsonProperty
  public Long producerByteRate() {
    return producerByteRate;
  }

  @JsonProperty
  public Long consumerByteRate() {
    return consumerByteRate;
  }

  @JsonProperty
  public Double requestPercentage() {
    return requestPercentage;
  }

  @JsonProperty
  public Integer networkQuotaOverhead() {
    return networkQuotaOverhead;
  }

  /**
   * Returns true if metadata values are valid
   */
  boolean isValid() {
    return (KAFKA_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType) ||
            HEALTHCHECK_LOGICAL_CLUSTER_TYPE.equals(logicalClusterType)) &&
            producerByteRate != null && consumerByteRate != null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogicalClusterMetadata that = (LogicalClusterMetadata) o;
    return Objects.equals(logicalClusterId, that.logicalClusterId) &&
           Objects.equals(physicalClusterId, that.physicalClusterId) &&
           Objects.equals(logicalClusterName, that.logicalClusterName) &&
           Objects.equals(accountId, that.accountId) &&
           Objects.equals(k8sClusterId, that.k8sClusterId) &&
           Objects.equals(logicalClusterType, that.logicalClusterType) &&
           Objects.equals(storageBytes, that.storageBytes) &&
           Objects.equals(producerByteRate, that.producerByteRate) &&
           Objects.equals(consumerByteRate, that.consumerByteRate) &&
           Objects.equals(requestPercentage, that.requestPercentage) &&
           Objects.equals(networkQuotaOverhead, that.networkQuotaOverhead);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        logicalClusterId, physicalClusterId, logicalClusterName, accountId, k8sClusterId,
        logicalClusterType, storageBytes, producerByteRate, consumerByteRate, requestPercentage,
        networkQuotaOverhead
    );
  }

  @Override
  public String toString() {
    return "LogicalClusterMetadata(logicalClusterId=" + logicalClusterId +
           ", physicalClusterId=" + physicalClusterId +
           ", logicalClusterName=" + logicalClusterName +
           ", accountId=" + accountId + ", k8sClusterId=" + k8sClusterId +
           ", logicalClusterType=" + logicalClusterType + ", storageBytes=" + storageBytes +
           ", producerByteRate=" + producerByteRate + ", consumerByteRate=" + consumerByteRate +
           ", requestPercentage=" + requestPercentage +
           ", networkQuotaOverhead=" + networkQuotaOverhead +
           ')';
  }
}
