// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Properties;

public class TopicPolicyConfig extends AbstractConfig {

  public static final String BASE_PREFIX = "confluent.plugins.";
  public static final String TOPIC_PREFIX = TopicPolicyConfig.BASE_PREFIX + "topic.policy.";

  public static final String REPLICATION_FACTOR_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "replication.factor";
  protected static final String REPLICATION_FACTOR_DOC =
      "The required replication factor for all topics if set.";

  public static final String MIN_IN_SYNC_REPLICAS_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "min.insync.replicas";
  protected static final String MIN_IN_SYNC_REPLICAS_CONFIG_DOC =
      "The required min.insync.replicas value if set.";

  public static final String MAX_PARTITIONS_PER_TENANT_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "max.partitions.per.tenant";
  public static final int DEFAULT_MAX_PARTITIONS_PER_TENANT = 512;
  protected static final String MAX_PARTITIONS_PER_TENANT_CONFIG_DOC =
      "The maximum partitions per tenant.";

  public static final String INTERNAL_LISTENER_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "internal.listener";
  public static final String DEFAULT_INTERNAL_LISTENER = "INTERNAL";
  protected static final String INTERNAL_LISTENER_CONFIG_DOC =
      "Internal listener to get bootstrap broker for AdminClient.";

  private static final ConfigDef config;

  static {
    config = new ConfigDef()
        .define(REPLICATION_FACTOR_CONFIG,
            ConfigDef.Type.SHORT,
            ConfigDef.Importance.HIGH,
            REPLICATION_FACTOR_DOC
        ).define(MIN_IN_SYNC_REPLICAS_CONFIG,
            ConfigDef.Type.SHORT,
            ConfigDef.Importance.HIGH,
            MIN_IN_SYNC_REPLICAS_CONFIG_DOC
        ).define(MAX_PARTITIONS_PER_TENANT_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_MAX_PARTITIONS_PER_TENANT,
            ConfigDef.Importance.HIGH,
            MAX_PARTITIONS_PER_TENANT_CONFIG_DOC
        ).define(INTERNAL_LISTENER_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_INTERNAL_LISTENER,
            ConfigDef.Importance.HIGH,
            INTERNAL_LISTENER_CONFIG_DOC
        );
  }

  public TopicPolicyConfig(Map<String, ?> clientConfigs) {
    super(config, clientConfigs);
  }

  public TopicPolicyConfig(Properties props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toRst());
  }
}
