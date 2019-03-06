// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.common.InterClusterConnection;
import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CreateTopicPolicy implements org.apache.kafka.server.policy.CreateTopicPolicy {

  private static final Logger log =
          LoggerFactory.getLogger(CreateTopicPolicy.class);


  private static final int TIMEOUT_MS = 60 * 1000;

  private short requiredRepFactor;
  private int maxPartitionsPerTenant;
  private TopicPolicyConfig policyConfig;

  Map<String, String> adminClientProps = new HashMap<>();

  @Override
  public void configure(Map<String, ?> cfgMap) {
    this.policyConfig = new TopicPolicyConfig(cfgMap);

    requiredRepFactor = policyConfig.getShort(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG);
    maxPartitionsPerTenant =
            policyConfig.getInt(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG);

    String listener = policyConfig.getString(TopicPolicyConfig.INTERNAL_LISTENER_CONFIG);

    // get bootstrap broker from internal listener config
    String bootstrapBroker = InterClusterConnection.getBootstrapBrokerForListener(listener, cfgMap);

    // we currently support internal listener security config = PLAINTEXT;
    // TODO: support other security configs
    String securityProtocol = InterClusterConnection.getListenerSecurityProtocol(listener, cfgMap);
    if (securityProtocol == null || securityProtocol.compareTo("PLAINTEXT") != 0) {
      throw new ConfigException(
          String.format("Expected %s listener security config = PLAINTEXT, got %s",
                        listener, securityProtocol));
    }

    log.debug("Using bootstrap servers {} for retrieving tenant's broker and partitions counts",
                 bootstrapBroker);
    adminClientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
  }

  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    // Only apply policy to tenant topics
    if (!TenantContext.isTenantPrefixed(reqMetadata.topic())) {
      return;
    }
    if (reqMetadata.numPartitions() == null) {
      throw new PolicyViolationException("Must specify number of partitions.");
    }

    Short repFactorPassed = reqMetadata.replicationFactor();
    if (repFactorPassed != null && repFactorPassed != requiredRepFactor) {
      throw new PolicyViolationException("Topic replication factor must be " + requiredRepFactor);
    }

    this.policyConfig.validateTopicConfigs(reqMetadata.configs());

    Map<String, Object> adminConfig = new HashMap<>(adminClientProps);
    log.debug("Checking partitions count with config: {}", adminClientProps);
    try (AdminClient adminClient = AdminClient.create(adminConfig)) {
      ensureValidPartitionCount(
              adminClient,
              TenantContext.extractTenantPrefix(reqMetadata.topic()),
              reqMetadata.numPartitions()
      );
    }
  }

  @Override
  public void close() throws Exception {}

  /**
   * Returns current number of topic partitions for this tenant
   */
  int numPartitions(AdminClient adminClient, String tenantPrefix) {
    ListTopicsOptions listTopicsOptions = new ListTopicsOptions().timeoutMs(TIMEOUT_MS);
    DescribeTopicsOptions describeTopicsOptions = new DescribeTopicsOptions().timeoutMs(TIMEOUT_MS);
    int totalCurrentPartitions = 0;
    try {
      ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
      Collection<String> topicNames = result.names().get();
      log.debug("Topics: {}", topicNames != null ? topicNames : "[]");
      if (topicNames != null) {
        DescribeTopicsResult topicsResult = adminClient.describeTopics(topicNames,
                describeTopicsOptions);
        Map<String, TopicDescription> topicDescriptionMap = topicsResult.all().get();
        if (topicDescriptionMap != null) {
          for (TopicDescription topicDescription : topicDescriptionMap.values()) {
            if (topicDescription.partitions() != null
                && TenantContext.hasTenantPrefix(tenantPrefix, topicDescription.name())) {
              totalCurrentPartitions += topicDescription.partitions().size();
            }
          }
        }
      }
    } catch (Exception e) {
      // no retry here because AdminClient already retries several times
      log.error("Error getting topics descriptions for tenant prefix {}", tenantPrefix, e);
      throw new PolicyViolationException("Failed to validate number of partitions.");
    }
    return totalCurrentPartitions;
  }

  /**
   * Validates requested number of partitions. Total number of partitions (requested plus
   * current) should not exceed maximum allowed number of partitions
   *
   * @param adminClient Kafka admin client
   * @param tenantPrefix topic prefix for tenant
   * @param partitionsCount requested number of partitions or the delta if validating a createPartitions request
   * @throws PolicyViolationException if requested number of partitions cannot be created
   */
  void ensureValidPartitionCount(AdminClient adminClient,
                                 String tenantPrefix,
                                 int partitionsCount) throws PolicyViolationException {
    if (partitionsCount > maxPartitionsPerTenant) {
      throw new PolicyViolationException(String.format(
          "You may not create more than the maximum number of partitions (%d).",
          maxPartitionsPerTenant));
    } else {
      int totalCurrentPartitions = numPartitions(adminClient, tenantPrefix);
      if (totalCurrentPartitions + partitionsCount > maxPartitionsPerTenant) {
        throw new PolicyViolationException(String.format(
            "You may not create more than %d new partitions. "
                + "Adding the requested number of partitions will exceed %d total partitions. "
                + "Currently, there are %d total topic partitions",
            maxPartitionsPerTenant - totalCurrentPartitions,
            maxPartitionsPerTenant, totalCurrentPartitions));
      }
      log.debug(
          "Validated adding {} partitions to {} current partitions (total={}, max={}) for {}",
          partitionsCount, totalCurrentPartitions, totalCurrentPartitions + partitionsCount,
          maxPartitionsPerTenant, tenantPrefix);
    }
  }

}
