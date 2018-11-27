/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions

import org.apache.kafka.common.TopicPartition

class TierArchiverFencedException(topicPartition: TopicPartition)
  extends RuntimeException(s"Fenced for partition $topicPartition")

class TierArchiverFatalException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause) {

  def this(topicPartition: TopicPartition, cause: Throwable) {
    this(s"Fatal exception for $topicPartition", cause)
  }
}
