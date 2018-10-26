// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;

public class AlterConfigPolicy implements org.apache.kafka.server.policy.AlterConfigPolicy {
  TopicPolicyConfig policyConfig;

  @Override
  public void configure(Map<String, ?> cfgMap) {
    this.policyConfig = new TopicPolicyConfig(cfgMap);
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
    this.policyConfig.validateTopicConfigs(reqMetadata.configs());
  }

  @Override
  public void close() throws Exception {}

}
