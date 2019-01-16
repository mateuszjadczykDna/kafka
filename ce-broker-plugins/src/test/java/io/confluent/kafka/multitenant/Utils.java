package io.confluent.kafka.multitenant;

import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class Utils {
  static final LogicalClusterMetadata LC_META_XYZ =
      new LogicalClusterMetadata("lkc-xyz", "pkc-xyz", "xyz",
          "my-account", "k8s-abc",
          104857600L, 1024L, 2048L,
          LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
          LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);
  public static final LogicalClusterMetadata LC_META_ABC =
      new LogicalClusterMetadata("lkc-abc", "pkc-abc", "abc",
          "my-account", "k8s-abc",
          10485760L, 102400L, 204800L,
          LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
          LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);

  public static PhysicalClusterMetadata initiatePhysicalClusterMetadata(Map<String, Object> configs) throws IOException {
    PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);

    return metadata;
  }

  static Path createLogicalClusterFile(LogicalClusterMetadata lcMeta, TemporaryFolder tempFolder) throws IOException {
    return createLogicalClusterFile(lcMeta, true, tempFolder);
  }

  public static Path createLogicalClusterFile(LogicalClusterMetadata lcMeta, boolean valid, TemporaryFolder tempFolder)
      throws IOException {
    final String lcFilename = lcMeta.logicalClusterId() + ".json";
    final Path lcFile = tempFolder.newFile(lcFilename).toPath();
    Files.write(lcFile, logicalClusterJsonString(lcMeta, valid).getBytes());
    return lcFile;
  }

  static String logicalClusterJsonString(LogicalClusterMetadata lcMeta, boolean setQuotaOverhead, boolean setReqPercent) {
    String json = baseLogicalClusterJsonString(lcMeta);
    if (setQuotaOverhead) {
      json += ", \"network_quota_overhead\": " + lcMeta.networkQuotaOverhead();
    }
    if (setReqPercent) {
      json += ", \"request_percentage\": " + lcMeta.requestPercentage();
    }
    json += "}";

    return json;
  }

  static String logicalClusterJsonString(LogicalClusterMetadata lcMeta, boolean valid) {
    String json = baseLogicalClusterJsonString(lcMeta);
    if (valid) {
      json += "}";
    }
    return json;
  }

  private static String baseLogicalClusterJsonString(LogicalClusterMetadata lcMeta) {
    String json = "{" +
        "\"logical_cluster_id\": \"" + lcMeta.logicalClusterId() + "\"," +
        "\"physical_cluster_id\": \"" + lcMeta.physicalClusterId() + "\"," +
        "\"logical_cluster_name\": \"" + lcMeta.logicalClusterName() + "\"," +
        "\"account_id\": \"" + lcMeta.accountId() + "\"," +
        "\"k8s_cluster_id\": \"" + lcMeta.k8sClusterId() + "\"," +
        "\"logical_cluster_type\": \"" + lcMeta.logicalClusterType() + "\"";
    if (lcMeta.storageBytes() != null) {
      json += ", \"storage_bytes\": " + lcMeta.storageBytes();
    }
    if (lcMeta.producerByteRate() != null) {
      json += ", \"network_ingress_byte_rate\": " + lcMeta.producerByteRate();
    }
    if (lcMeta.consumerByteRate() != null) {
      json += ", \"network_egress_byte_rate\": " + lcMeta.consumerByteRate();
    }

    return json;
  }
}
