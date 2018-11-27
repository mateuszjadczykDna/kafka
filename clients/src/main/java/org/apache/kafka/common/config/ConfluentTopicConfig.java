/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.common.config;

/**
 * Keys that can be used to configure a topic for Confluent Platform Kafka. These keys are useful when creating or reconfiguring a
 * topic using the AdminClient.
 *
 */
// This is a public API, so we should not remove or alter keys without a discussion and a deprecation period.
public class ConfluentTopicConfig {
    public static final String TIER_ENABLE_CONFIG = "tier.enable";
    public static final String TIER_ENABLE_DOC = "True if this topic has tiered storage enabled.";
}
