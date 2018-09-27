// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;

public class AlterConfigPolicy implements org.apache.kafka.server.policy.AlterConfigPolicy {
  private short requiredMinIsrs = 2;

  @Override
  public void configure(Map<String, ?> cfgMap) {
    TopicPolicyConfig policyConfig = new TopicPolicyConfig(cfgMap);
    requiredMinIsrs = policyConfig.getShort(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
  }


  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    if (reqMetadata.resource().type().equals(ConfigResource.Type.TOPIC)) {
      validateTopicRequest(reqMetadata);
    } else {
      throw new PolicyViolationException(String.format(
              "Altering resources of type '%s' is not permitted",
              reqMetadata.resource().toString()));
    }
  }

  void validateTopicRequest(RequestMetadata reqMetadata) throws PolicyViolationException {
    Map<String, String> configs = reqMetadata.configs();
    if (configs != null && configs.containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
      short minIsrsPassed = Short.parseShort(configs.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
      if (minIsrsPassed != requiredMinIsrs) {
        throw new PolicyViolationException(String.format("Topic config '%s' must be %s",
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
            requiredMinIsrs));
      }
    }
  }

  @Override
  public void close() throws Exception {}

}
