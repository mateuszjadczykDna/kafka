/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.util
import java.util.{Collections, UUID}

import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.serdes.State
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class TierTopicManagerTest {
  val tierTopicName = "__tier_topic"
  val tierTopicNumPartitions = 1: Short
  val clusterId = "mycluster"
  val tempDir = TestUtils.tempDir()
  val logDir = tempDir.getAbsolutePath
  val logDirs = new util.ArrayList(util.Collections.singleton(logDir))

  @Test
  def testTierTopicManager(): Unit = {
    try {
      val tierTopicManagerConfig = new TierTopicManagerConfig(
        "",
        "",
        tierTopicNumPartitions,
        1,
        3,
        clusterId,
        5,
        30000,
        logDirs)

      val producerBuilder = new MockProducerBuilder()
      val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig,
        producerBuilder.producer())

      val tierTopicManager = new TierTopicManager(
        tierTopicManagerConfig,
        consumerBuilder,
        producerBuilder,
        new FileTierPartitionStateFactory(logDir, 0.001))

      tierTopicManager.becomeReady()

      val archivedPartition1 = new TopicPartition("archivedTopic", 0)

      addReplica(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.ACCEPTED)

      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1.topic(),
          archivedPartition1.partition(),
          0,
          0L,
          1000,
          15000L,
          16000L,
          1000,
          1000,
          false,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      val tierPartitionState = tierTopicManager.getPartitionState(archivedPartition1)
      assertEquals(1000L, tierPartitionState.endOffset().getAsLong)

      // should be ignored
      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1.topic(),
          archivedPartition1.partition(),
          0,
          0L,
          1000,
          15000L,
          16000L,
          2000L,
          1000,
          true,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      // end offset shouldn't have updated - message with identical ranges with same start offset and epoch
      // should be filtered
      assertEquals(1000L, tierPartitionState.totalSize)

      // should be ignored
      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1.topic(),
          archivedPartition1.partition(),
          0,
          1L,
          1001,
          15000L,
          16000L,
          2000L,
          1000,
          false,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()

      // end offset shouldn't have updated - message with overlapping ranges with same epoch
      // should be filtered
      assertEquals(1000L, tierPartitionState.totalSize)

      // test rejoin on the same epoch
      addReplica(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.ACCEPTED)

      // larger epoch should be accepted
      addReplica(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        1,
        AppendResult.ACCEPTED)

      // going back to previous epoch should be fenced
      addReplica(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.FENCED)

      // add a second partition and ensure it catches up.
      val archivedPartition2 = new TopicPartition("archivedTopic", 1)
      addReplica(consumerBuilder,
        tierTopicManager,
        archivedPartition2,
        0,
        AppendResult.ACCEPTED)

      tierTopicManager.committer.flush()
      assertEquals(tierTopicManager.committer.positions.get(0),
        consumerBuilder.logEndOffset)

      assertFalse(tierTopicManager.catchingUp())
    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  private def addReplica(consumerBuilder: MockConsumerBuilder,
                         tierTopicManager: TierTopicManager,
                         topicPartition: TopicPartition,
                         epoch: Integer,
                         expected: AppendResult): Unit = {
    tierTopicManager.immigratePartitions(
      Collections.singletonList(topicPartition))
    // force immigration
    tierTopicManager.doWork()
    val result = tierTopicManager.becomeArchiver(topicPartition, epoch)
    consumerBuilder.moveRecordsFromProducer()
    while (!tierTopicManager.doWork()) assertEquals(expected, result.get())
  }
}
