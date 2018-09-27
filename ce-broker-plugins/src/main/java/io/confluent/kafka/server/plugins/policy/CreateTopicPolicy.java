// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.schema.TenantContext;
import kafka.security.auth.Create;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CreateTopicPolicy implements org.apache.kafka.server.policy.CreateTopicPolicy {

  private static final Logger logger =
          LoggerFactory.getLogger(CreateTopicPolicy.class);

  private static final String ADVERTISED_LISTENERS_CONFIG = "advertised.listeners";
  private static final String LISTENER_SECURITY_PROTOCOL = "listener.security.protocol.map";
  private static final int TIMEOUT_MS = 500;

  private short requiredRepFactor = 3;
  private short requiredMinIsrs = 2;
  private int maxPartitionsPerTenant = 512;
  Map<String, String> adminClientProps = new HashMap<>();

  @Override
  public void configure(Map<String, ?> cfgMap) {
    TopicPolicyConfig policyConfig = new TopicPolicyConfig(cfgMap);
    requiredRepFactor = policyConfig.getShort(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG);
    requiredMinIsrs = policyConfig.getShort(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    maxPartitionsPerTenant =
            policyConfig.getInt(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG);
    String listener = policyConfig.getString(TopicPolicyConfig.INTERNAL_LISTENER_CONFIG);

    // get bootstrap broker from internal listener config
    String bootstrapBroker = getBootstrapBrokerForListener(listener, cfgMap);

    // we currently support internal listener security config = PLAINTEXT;
    // TODO: support other security configs
    String securityProtocol = getListenerSecurityProtocol(listener, cfgMap);
    if (securityProtocol == null || securityProtocol.compareTo("PLAINTEXT") != 0) {
      throw new ConfigException(
          String.format("Expected %s listener security config = PLAINTEXT, got %s",
                        listener, securityProtocol));
    }

    logger.debug("Using bootstrap servers {} for retrieving tenant's broker and partitions counts",
                 bootstrapBroker);
    adminClientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
  }

  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    //Only apply policy to tenant topics
    if (!TenantContext.isTenantPrefixed(reqMetadata.topic())) {
      return;
    }
    Short repFactorPassed = reqMetadata.replicationFactor();
    if (repFactorPassed != null && repFactorPassed != requiredRepFactor) {
      throw new PolicyViolationException("Topic replication factor must be " + requiredRepFactor);
    }
    Map<String, String> configs = reqMetadata.configs();
    if (configs != null && configs.containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
      short minIsrsPassed = Short.parseShort(configs.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
      if (minIsrsPassed != requiredMinIsrs) {
        throw new PolicyViolationException(String.format("Topic config '%s' must be %s",
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
            requiredMinIsrs));
      }
    }
    if (reqMetadata.numPartitions() != null) {
      Map<String, Object> adminConfig = new HashMap<>();
      adminConfig.putAll(adminClientProps);
      logger.debug("Checking partitions count with config: {}", adminClientProps);
      try (AdminClient adminClient = AdminClient.create(adminConfig)) {
        ensureValidPartitionCount(
            adminClient,
            TenantContext.extractTenantPrefix(reqMetadata.topic()),
            reqMetadata.numPartitions()
        );
      }
    } else {
      throw new PolicyViolationException("Must specify number of partitions.");
    }
  }

  @Override
  public void close() throws Exception {}

  String getBootstrapBrokerForListener(String listener, Map<String, ?> cfgMap)
          throws ConfigException {
    String bootstrapBroker = null;
    if (cfgMap.get(ADVERTISED_LISTENERS_CONFIG) != null) {
      if (cfgMap.get(ADVERTISED_LISTENERS_CONFIG) instanceof String) {
        final int brokerStartIndex = listener.length() + 3;
        final String advertisedListeners = (String) cfgMap.get(ADVERTISED_LISTENERS_CONFIG);
        String[] listeners = advertisedListeners.split(",");
        for (String advertisedListener: listeners) {
          if (advertisedListener.contains(listener)
                  && advertisedListener.length() > brokerStartIndex) {
            bootstrapBroker = advertisedListener.substring(brokerStartIndex);
            break;
          }
        }
      }
      if (bootstrapBroker == null) {
        throw new ConfigException(String.format(
            "Expected to find %s listener in advertised.listeners config", listener));
      }

    } else {
      throw new ConfigException(
          String.format("%s config is required to validate topic creation",
                  ADVERTISED_LISTENERS_CONFIG));
    }

    return bootstrapBroker;
  }

  String getListenerSecurityProtocol(String listener, Map<String, ?> cfgMap)
          throws ConfigException {
    String protocol = null;
    if (cfgMap.get(LISTENER_SECURITY_PROTOCOL) != null
          && cfgMap.get(LISTENER_SECURITY_PROTOCOL) instanceof String) {
      final int protocolStartIndex = listener.length() + 1;
      final String listenerSecurityProtocols = (String) cfgMap.get(LISTENER_SECURITY_PROTOCOL);
      String[] securityProtocolsList = listenerSecurityProtocols.split(",");
      for (String securityProtocol: securityProtocolsList) {
        if (securityProtocol.contains(listener)
                && securityProtocol.length() > protocolStartIndex) {
          protocol = securityProtocol.substring(protocolStartIndex);
          break;
        }
      }
    }
    return protocol;
  }

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
      logger.debug("Topics: {}", (topicNames != null ? topicNames : "[]"));
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
      logger.error("Error getting topics descriptions for tenant prefix {}", tenantPrefix, e);
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
   * @param partitionsCount requested number of partitions
   * @throws PolicyViolationException if requested number of partitions cannot be created
   */
  void ensureValidPartitionCount(AdminClient adminClient,
                                 String tenantPrefix,
                                 int partitionsCount) throws PolicyViolationException {
    if (partitionsCount > maxPartitionsPerTenant) {
      throw new PolicyViolationException(String.format(
          "You may not create more than maximum number of partitions (%d).",
          maxPartitionsPerTenant));
    } else {
      int totalCurrentPartitions = numPartitions(adminClient, tenantPrefix);
      if (totalCurrentPartitions + partitionsCount > maxPartitionsPerTenant) {
        throw new PolicyViolationException(String.format(
            "You may not create more than %d partitions. "
                + "Adding requested number of partitions will exceed %d total partitions. "
                + "Currently, there are %d total topic partitions",
            maxPartitionsPerTenant - totalCurrentPartitions,
            maxPartitionsPerTenant, totalCurrentPartitions));
      }
      logger.debug(
          "Validated adding {} partitions to {} current partitions (total={}, max={}) for {}",
          partitionsCount, totalCurrentPartitions, totalCurrentPartitions + partitionsCount,
          maxPartitionsPerTenant, tenantPrefix);
    }
  }

}
