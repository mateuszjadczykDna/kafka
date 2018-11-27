/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.serdes.State
import kafka.tier.state.{FileTierPartitionState, MemoryTierPartitionState, TierPartitionStatus}
import kafka.utils.ScalaCheckUtils.assertProperty
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters.defaultVerbose

class TierPartitionStatePropertyTest {
  val brokerId = 0
  val baseDir = System.getProperty("java.io.tmpdir")
  val topic = UUID.randomUUID().toString
  val partition = 0
  val tp = new TopicPartition(topic, partition)
  val n = 200

  val genObjectMetadata: Gen[TierObjectMetadata] = for {
    version <- Gen.posNum[Int]
    epoch <- Gen.posNum[Int]
    startOffset <- Gen.posNum[Long]
    endOffsetDelta <- Gen.posNum[Int]
    lastStableOffset <- Gen.posNum[Long]
    hasAborts <- Gen.oneOf(true, false)
    maxTimestamp <- Gen.posNum[Long]
    lastModifiedTime <- Gen.posNum[Long]
    size <- Gen.posNum[Int]
  } yield
    new TierObjectMetadata(topic,
                           partition,
                           epoch,
                           startOffset,
                           endOffsetDelta,
                           lastStableOffset,
                           maxTimestamp,
                           lastModifiedTime,
                           size,
                           hasAborts,
                           State.AVAILABLE)

  val genInit: Gen[TierTopicInitLeader] = for {
    version <- Gen.posNum[Int]
    epoch <- Gen.posNum[Int]} yield
    new TierTopicInitLeader(topic,
                            partition,
                            epoch,
                            UUID.randomUUID(),
                            brokerId)

  val genMetadata: Gen[AbstractTierMetadata] =
    Gen.oneOf(genObjectMetadata, genInit)

  @Test
  def testSameElementsProperty(): Unit = {
    val prop = forAll(Gen.listOf(genMetadata)) {
      objectMetadatas =>
        val diskstate = new FileTierPartitionState(baseDir, tp, 0.01)
        diskstate.targetStatus(TierPartitionStatus.CATCHUP)
        diskstate.targetStatus(TierPartitionStatus.ONLINE)
        try {
          val memstate = new MemoryTierPartitionState(tp)
          memstate.targetStatus(TierPartitionStatus.CATCHUP)
          memstate.targetStatus(TierPartitionStatus.ONLINE)
          for (m <- objectMetadatas) {
            memstate.append(m)
            diskstate.append(m)
          }
          memstate.totalSize == diskstate.totalSize
        } finally {
          diskstate.close()
          diskstate.delete()
        }
    }

    assertProperty(prop, defaultVerbose.withMinSuccessfulTests(2000))
  }

  case class OffsetCheck(metadatas: List[AbstractTierMetadata],
                         offset: Long,
                         indexFrequency: Long)

  val genOffsetCheck: Gen[OffsetCheck] = for {
    numEntries <- Gen.choose(0, 100)
    metadatas <- Gen.listOfN(numEntries, genMetadata)
    offset <- Gen.choose(0, 100000)
    indexFrequency <- Gen.choose(1, 1000)
  } yield OffsetCheck(metadatas, offset, indexFrequency)

  @Test
  def testMetadataForOffsetProperty(): Unit = {
    val prop = forAll(genOffsetCheck) { trial =>
      val diskstate = new FileTierPartitionState(baseDir, tp, 1.0 / trial.indexFrequency)
      diskstate.targetStatus(TierPartitionStatus.CATCHUP)
      diskstate.targetStatus(TierPartitionStatus.ONLINE)
      try {
        val memstate = new MemoryTierPartitionState(tp)
        memstate.targetStatus(TierPartitionStatus.CATCHUP)
        memstate.targetStatus(TierPartitionStatus.ONLINE)
        for (m <- trial.metadatas) {
          memstate.append(m)
          diskstate.append(m)
        }
        val m1 = memstate.getObjectMetadataForOffset(trial.offset)
        val m2 = diskstate.getObjectMetadataForOffset(trial.offset)

        m1.equals(m2)

      } finally {
        diskstate.close()
        diskstate.delete()
      }
    }

    assertProperty(prop, defaultVerbose.withMinSuccessfulTests(2000))
  }
}
