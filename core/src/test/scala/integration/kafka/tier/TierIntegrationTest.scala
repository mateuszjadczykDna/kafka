/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.function.Consumer

import kafka.log.{Log, LogConfig, LogManager, LogTest}
import kafka.server.{BrokerTopicStats, ReplicaManager}
import kafka.tier.archiver.{TierArchiver, TierArchiverConfig, TierArchiverState}
import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.state.MemoryTierPartitionStateFactory
import kafka.tier.store.MockInMemoryTierObjectStore
import kafka.tier.store.TierObjectStore.TierObjectStoreFileType
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._

class TierIntegrationTest {
  private val mockTime = new MockTime()
  val logDirs = new util.ArrayList(Collections.singleton(System.getProperty("java.io.tmpdir")))

  val tierTopicManagerConfig = new TierTopicManagerConfig(
    "bootstrap",
    null,
    1,
    1,
    33,
    "cluster99",
    10L,
    500,
    logDirs
  )

  @Test
  def testArchiverEmigrate(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(2, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, 1) }

    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)
    while (tierTopicManager.doWork()) {}

    assertEquals("Topic partitions should be queued for immigration.", logs.size, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("No transitions should be in progress.", 0, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    assertEquals("No topic partitions should be queued.", 0, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("Topic partitions should be transitioning to the next status.", logs.size, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "Topic manager should process init entries", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should advance both partitions to BeforeUpload status", 2000L)

    // Emigrate one partition
    tierArchiver.handleEmigration(logs.head.topicPartition)

    assertEquals("One topic partition should be queued for emigration.", 1, tierArchiver.immigrationEmigrationQueue.size())

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending emigrations", 2000L)

    assertEquals("No topic partitions should be queued for emigration.", 0, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("Remaining topic partition should be transitioning to the next status.", 1, tierArchiver.stateTransitionsInProgress.size)

    // Re-immigrate with valid epoch
    tierArchiver.handleImmigration(logs.head.topicPartition, 2)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    assertEquals("All topic partitions should be transitioning to the next status.", logs.size, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "Topic manager should process an init entry", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should advance one partition to the BeforeUploadState", 2000L)
  }

  @Test
  def testArchiverFencing(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(1, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    tierTopicManager.becomeArchiver(logs.head.topicPartition, 2)

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, 1) }
    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)
    while (tierTopicManager.doWork()) {}

    assertEquals("Topic partitions should be queued for immigration.", logs.size, tierArchiver.immigrationEmigrationQueue.size())

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "Topic manager should process init entries", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should handle fenced exception", 2000L)
  }

  @Test
  def testArchiverUploadAndMaterialize(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    val numBatches = 6

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(10, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    val leaderEpoch = 1

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, leaderEpoch) }
    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)
    while (tierTopicManager.doWork()) {}

    assertEquals("Topic partitions should be queued for immigration.", logs.size, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("No transitions should be in progress.", 0, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    assertEquals("No topic partitions should be queued.", 0, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("Topic partitions should be transitioning to the next status.", logs.size, tierArchiver.stateTransitionsInProgress.size)


    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, numBatches, 4) }

    // Become leader
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize init entries", 2000L)

    logs.foreach { log =>
      assertEquals(s"topic manager should materialize entry for ${log.topicPartition}",
        tierTopicManager.getPartitionState(log.topicPartition).tierEpoch, leaderEpoch)
    }

    assertEquals(0, tierObjectStore.getStored.keySet().size())

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: could not run all pending states",
      1000L
    )
    // Archive batch 1
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should transition to BeforeUpload status for each topic partition",
      1000L)


    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should run pending BeforeUpload states",
      1000L
    )
    // Archive batch 1
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause resulting AfterUpload states",
      1000L)

    assertEquals(60, tierObjectStore.getStored.keySet().size())

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should transition states through AfterUpload back to BeforeUpload",
      1000L
    )
    // Archive batch 1
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "batch 1: topic manager should process the object metadata entries", 2000L)


    logs.foreach { log =>
      assertEquals("batch 1: segment should be materialized with correct offset relationship",
        0, tierTopicManager.getPartitionState(log.topicPartition).getObjectMetadataForOffset(0).get().startOffset)
      assertEquals("batch 1: segment should be materialized with correct end offset",
        3, tierTopicManager.getPartitionState(log.topicPartition).endOffset.getAsLong)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning BeforeUpload to AfterUpload",
      1000L
    )
    // Archive batch 1
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on AfterUpload",
      1000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning AfterUpload to BeforeUpload",
      1000L
    )
    // Archive batch 1
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
    assertEquals(120, tierObjectStore.getStored.keySet().size())

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning BeforeUpload to AfterUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on AfterUpload",
      1000L)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "batch 1: topic manager should process the object metadata entries", 2000L)

    logs.foreach { log =>
      assertEquals("batch 2: segment should be materialized with correct offset relationship",
        4, tierTopicManager.getPartitionState(log.topicPartition).getObjectMetadataForOffset(6).get().startOffset)
      assertEquals("batch 2: segment should be materialized with correct end offset",
        7, tierTopicManager.getPartitionState(log.topicPartition).endOffset.getAsLong)
    }

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning states from AfterUpload to BeforeUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)


    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    assertEquals(120, tierObjectStore.getStored.keySet().size())

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning BeforeUpload to AfterUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on AfterUpload",
      1000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning states from AfterUpload to BeforeUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)


    assertEquals(180, tierObjectStore.getStored.keySet().size())

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "batch 3: topic manager should process the object metadata entry", 2000L)

    logs.foreach { log =>
      assertEquals("batch 3: segment should be materialized with correct offset relationship",
        8, tierTopicManager.getPartitionState(log.topicPartition).getObjectMetadataForOffset(10).get().startOffset)
      assertEquals("batch 3: segment should be materialized with correct end offset",
        11, tierTopicManager.getPartitionState(log.topicPartition).endOffset.getAsLong)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

  }

  @Test
  def testArchiverUploadAndMaterializeWhenWriteHappensAfterBecomeLeader(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(10, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    val leaderEpoch = 1

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, leaderEpoch) }
    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)
    // process immigration
    while (tierTopicManager.doWork()) {}

    assertEquals("Topic partitions should be queued for immigration.", logs.size, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("No transitions should be in progress.", 0, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    assertEquals("No topic partitions should be queued.", 0, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("Topic partitions should be transitioning to the next status.", logs.size, tierArchiver.stateTransitionsInProgress.size)

    // Become leader
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize init entries", 2000L)

    logs.foreach { log =>
      assertEquals(s"topic manager should materialize entry for ${log.topicPartition}",
        leaderEpoch, tierTopicManager.getPartitionState(log.topicPartition).tierEpoch)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 6, 4) }


    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning BeforeUpload to AfterUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on AfterUpload",
      1000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning states from AfterUpload to BeforeUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning states from BeforeUpload to BeforeUpload",
      1000L
    )

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "batch 1: topic manager should process the object metadata entries", 2000L)

    logs.foreach { log =>
      assertEquals("batch 1: segment should be materialized with correct offset relationship",
        0, tierTopicManager.getPartitionState(log.topicPartition).getObjectMetadataForOffset(0).get().startOffset)
      assertEquals("batch 1: segment should be materialized with correct end offset",
        3, tierTopicManager.getPartitionState(log.topicPartition).endOffset.getAsLong)

      validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
    }
  }


  @Test
  def testArchiverUploadWithLimitedUploadConcurrency(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(3, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(maxConcurrentUploads = 2), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    val leaderEpoch = 1

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, leaderEpoch) }
    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)
    // process immigration
    while (tierTopicManager.doWork()) {}

    // Write batches to all partitions
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 2, 4) }

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process immigrations", 2000L)

    // Become leader for all partitions
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize first two init entries", 2000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start transitioning states to BeforeUpload",
      1000L
    )
    assertEquals("2 status transitions should be in progress at a time", 2, tierArchiver.stateTransitionsInProgress.size)
    assertEquals("One status transition should be paused", 1, tierArchiver.pausedStates.size())
    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states on BeforeUpload",
      1000L)

    assertEquals("All 3 status transitions should be paused, 2 in BeforeUpload and 1 in BeforeLeader", 3, tierArchiver.pausedStates.size())

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should start to advance the one remaining BeforeLeader status to BeforeUpload, and one BeforeUpload status to AfterUpload",
      1000L
    )

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize second init entry", 2000L)


    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states, we should have 2 states in BeforeUpload, and 1 status in AfterUpload",
      1000L)

    var afterUploadCount = 0
    var beforeUploadCount = 0
    tierArchiver.pausedStates.forEach(new Consumer[TierArchiverState] {
      override def accept(t: TierArchiverState): Unit = {
        t match {
          case _: TierArchiverState.BeforeUpload =>
            beforeUploadCount += 1
          case _: TierArchiverState.AfterUpload =>
            afterUploadCount += 1
          case _ =>
            assertTrue("No other states expected", false)
        }
      }
    })
    assertEquals("", afterUploadCount, 1)
    assertEquals("", beforeUploadCount, 2)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.tryRunPendingStates(),
      "batch 1: archiver should advance the 1 AfterUpload status to BeforeUpload and 1 of the 2 BeforeUpload states to AfterUpload",
      1000L
    )

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize second init entry", 2000L)

    TestUtils.waitUntilTrue(
      () => !tierArchiver.pauseDoneStates(),
      "batch 1: archiver should pause all states, resulting in 1 AfterUpload status and 2 BeforeUpload states",
      1000L)

    afterUploadCount = 0
    beforeUploadCount = 0
    tierArchiver.pausedStates.forEach(new Consumer[TierArchiverState] {
      override def accept(t: TierArchiverState): Unit = {
        t match {
          case _: TierArchiverState.BeforeUpload =>
            beforeUploadCount += 1
          case _: TierArchiverState.AfterUpload =>
            afterUploadCount += 1
          case _ =>
            assertTrue("No other states expected", false)
        }
      }
    })
    assertEquals("unexpected AfterUpload count", 1, afterUploadCount)
    assertEquals("unexpected BeforeUpload count", 2, beforeUploadCount)
  }

  @Test
  def testArchiverUploadWithAsymmetricalWrites(): Unit = {
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager

    // Create replica manager and test logs
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(3, logConfig, tempDir)
    val replicaManager = mockReplicaManager(logs)

    val tierObjectStore = new MockInMemoryTierObjectStore("somebucket")

    val tierArchiver = new TierArchiver(TierArchiverConfig(maxConcurrentUploads = 2), replicaManager, tierTopicManager, tierObjectStore, mockTime)

    val leaderEpoch = 1

    // Write a batch only to the second two logs
    logs.tail.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 1, 4) }

    // Immigrate all test logs
    logs.foreach { log => tierArchiver.handleImmigration(log.topicPartition, leaderEpoch) }
    tierTopicManager.immigratePartitions(logs.map(_.topicPartition).toList.asJava)

    // process immigration
    TestUtils.waitUntilTrue(() => !tierArchiver.processImmigrationEmigrationQueue() && !tierArchiver.tryRunPendingStates(), "archiver should process immigrations", 2000L)
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize first two init entries", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.pauseDoneStates(), "archiver should pause 2 states in BeforeUpload, 1 in BeforeLeader")
    var beforeLeaderCount = 0
    var beforeUploadCount = 0
    tierArchiver.pausedStates.forEach(new Consumer[TierArchiverState] {
      override def accept(t: TierArchiverState): Unit = {
        t match {
          case _: TierArchiverState.BeforeUpload =>
            beforeUploadCount += 1
          case _: TierArchiverState.AfterUpload =>
            assertTrue("should not have uploded anything", false)
          case _: TierArchiverState.BeforeLeader =>
            beforeLeaderCount += 1
        }
      }
    })
    assertEquals("unexpected AfterUpload count", 1, beforeLeaderCount)
    assertEquals("unexpected BeforeUpload count", 2, beforeUploadCount)

    TestUtils.waitUntilTrue(() => !tierArchiver.tryRunPendingStates(), "Archiver should process next BeforeLeader entry", 2000L)
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "topic manager should materialize second init entry", 2000L)
    TestUtils.waitUntilTrue(() => tierArchiver.pauseDoneStates(), "archiver should see no more BeforeLeader entries")
    tierArchiver.pausedStates.forEach(new Consumer[TierArchiverState] {
      override def accept(t: TierArchiverState): Unit = {
        t match {
          case _: TierArchiverState.BeforeLeader =>
            assertTrue("should not see any more BeforeLeader states", false)
          case _ =>
        }
      }
    })

    TestUtils.waitUntilTrue(
      () => tierArchiver.processTransitions(),
      "Archiver should advance remaining partition to BeforeUpload",
      1000L)

    // Write ten batches to first log
    logs.headOption.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 10, 4) }

    TestUtils.waitUntilTrue(
      () => tierArchiver.processTransitions(),
      "Archiver should do work",
      1000L)

    assertTrue("First log should be uploading",
      tierArchiver.stateTransitionsInProgress.contains(logs.head.topicPartition))
  }


  private def validatePartitionStateContainedInObjectStore
  (tierTopicManager: TierTopicManager,
   tierObjectStore: MockInMemoryTierObjectStore,
   logs: Iterable[Log]): Unit = {
    logs.foreach { log =>
      val iterator = tierTopicManager.getPartitionState(log.topicPartition).iterator()
      while (iterator.hasNext) {
        val objectMetadata = iterator.next()
        val tierObjectMetadata = new TierObjectMetadata(log.topicPartition.topic(), log.topicPartition.partition(), objectMetadata)
        assertNotNull(tierObjectStore.getObject(tierObjectMetadata, TierObjectStoreFileType.SEGMENT, 0, 1000).getObjectSize)
      }
    }
  }


  private def setupTierTopicManager: (TierTopicManager, MockConsumerBuilder) = {
    val producerBuilder = new MockProducerBuilder()
    val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig, producerBuilder.producer())
    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      new MemoryTierPartitionStateFactory())
    tierTopicManager.becomeReady()
    (tierTopicManager, consumerBuilder)
  }

  private def createLogs(n: Int, logConfig: LogConfig, tempDir: File): IndexedSeq[Log] = {
    (0 until n).map { i =>
      val logDir = tempDir.toPath.resolve(s"tierlogtest-$i").toFile
      logDir.mkdir()
      LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
        0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)
    }
  }

  private def mockReplicaManager(logs: Iterable[Log]): ReplicaManager = {
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(new Answer[Option[Log]] {
        override def answer(invocation: InvocationOnMock): Option[Log] = {
          val target: TopicPartition = invocation.getArgument(0)
          logs.find { log => log.topicPartition == target }
        }
      })
    replicaManager
  }

  private def writeRecordBatches(log: Log, leaderEpoch: Int, baseOffset: Long, batches: Int, recordsPerBatch: Int): Unit = {
    (0 until batches).foreach { idx =>
      val records = createRecords(log.topicPartition, leaderEpoch, baseOffset + idx * recordsPerBatch, recordsPerBatch)
      log.appendAsFollower(records)
      log.flush()
      log.onHighWatermarkIncremented(batches * recordsPerBatch)
    }
  }

  private def createRecords(topicPartition: TopicPartition, leaderEpoch: Int, baseOffset: Long, numRecords: Int): MemoryRecords = {
    val recList = (0 until numRecords).map { _ =>
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes())
    }
    val records = TestUtils.records(records = recList, baseOffset = baseOffset)
    val filtered = ByteBuffer.allocate(100 * numRecords)
    records.batches().asScala.foreach(_.setPartitionLeaderEpoch(leaderEpoch))
    records.filterTo(topicPartition, new RecordFilter {
      override protected def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetention = RecordFilter.BatchRetention.DELETE_EMPTY

      override protected def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = true
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    MemoryRecords.readableRecords(filtered)
  }

}
