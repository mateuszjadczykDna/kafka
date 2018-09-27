// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlterConfigPolicyTest {
  private AlterConfigPolicy policy;
  private RequestMetadata requestMetadata;

  @Before
  public void setUp() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    policy = new AlterConfigPolicy();
    policy.configure(config);
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4")
        .build();
    requestMetadata = mock(RequestMetadata.class);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.TOPIC, "dummy");
    when(requestMetadata.resource()).thenReturn(cfgResource);
  }
  @Test
  public void validateParamsSetOk() throws Exception {
    policy.validate(requestMetadata);
  }

  @Test
  public void validateNoParamsGivenOk() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBadMinIsrs() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBrokerConfigs() throws Exception {
    RequestMetadata brokerRequestMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.BROKER, "dummy");
    when(brokerRequestMetadata.resource()).thenReturn(cfgResource);
    policy.validate(brokerRequestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsUnknownTypeConfigs() throws Exception {
    RequestMetadata brokerRequestMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.UNKNOWN, "dummy");
    when(brokerRequestMetadata.resource()).thenReturn(cfgResource);
    policy.validate(brokerRequestMetadata);
  }
}
