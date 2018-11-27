package kafka.server

import java.util.{Collections, Properties}

import kafka.integration.KafkaServerTestHarness
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.serdes.State
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.TierPartitionStatus
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

/* Temporary integration test until we have a more substantial Tiered Storage integration test with archiving. */
class TierTopicManagerIntegrationTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  overridingProps.setProperty("tier.feature", "true")
  overridingProps.setProperty("tier.metadata.num.partitions", "2")
  overridingProps.setProperty("tier.metadata.replication.factor", "1")

  override def generateConfigs =
    TestUtils
      .createBrokerConfigs(1, zkConnect, enableControlledShutdown = false)
      .map {
        KafkaConfig.fromProps(_, overridingProps)
      }

  @Test
  def testTierTopicManager(): Unit = {
    val tierTopicManager = servers.last.tierTopicManager

    while (!tierTopicManager.isReady) {
      Thread.sleep(5)
    }

    val topicPartition = new TopicPartition("foo", 0)
    tierTopicManager.immigratePartitions(Collections.singletonList(topicPartition))

    val initialState = tierTopicManager.getPartitionState(topicPartition)
    assertEquals(0, initialState.totalSize)
    assertEquals(TierPartitionStatus.INIT, initialState.status())

    val result = tierTopicManager.becomeArchiver(topicPartition, 0)
    assertEquals(AppendResult.ACCEPTED, result.get())

    val metaresult1 = tierTopicManager
      .addMetadata(
        new TierObjectMetadata(
          topicPartition.topic(),
          topicPartition.partition(),
          0,
          0L,
          1000,
          15000L,
          16000L,
          17000,
          100,
          true,
          State.AVAILABLE))
      .get()

    assertEquals(AppendResult.ACCEPTED, metaresult1)

    val tierPartitionState = tierTopicManager.getPartitionState(topicPartition)
    assertEquals(1000L, tierPartitionState.endOffset.getAsLong)
    val metaresult2 = tierTopicManager.addMetadata(
      new TierObjectMetadata(
        topicPartition.topic(),
        topicPartition.partition(),
        0,
        0L,
        1000,
        15000L,
        16000L,
        16001L,
        200,
        false,
        State.AVAILABLE))

    assertEquals(AppendResult.FENCED, metaresult2.get())
    assertEquals(1000L, tierPartitionState.endOffset.getAsLong)


    assertEquals(1, tierPartitionState.numSegments())

    val topicPartition2 = new TopicPartition("topic2", 1)
    tierTopicManager.immigratePartitions(Collections.singletonList(topicPartition2))
    val topicPartition2result1 = tierTopicManager.becomeArchiver(topicPartition2, 0)
    assertEquals(AppendResult.ACCEPTED, topicPartition2result1.get())

    // original tier partition state should only have one entry, even after catch up
    // consumer has been seeked backwards.
    assertEquals(1, tierPartitionState.numSegments())
    TestUtils.waitUntilTrue(() => {
      !tierTopicManager.catchingUp()
    }, "tierTopicManager consumers catchingUp timed out", 500L)
  }
}
