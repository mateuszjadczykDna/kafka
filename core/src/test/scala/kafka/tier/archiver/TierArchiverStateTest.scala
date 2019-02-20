/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.{ConcurrentSkipListSet, Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Optional, OptionalLong, Properties}

import kafka.log.{AbstractLog, LogManager, LogSegment, LogTest, MergedLog}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.tier.archiver.TierArchiverState.{AfterUpload, BeforeLeader, BeforeUpload, Priority}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{MemoryTierPartitionStateFactory, TierPartitionState}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore}
import kafka.tier.store.TierObjectStore.TierObjectStoreFileType
import kafka.tier.{TierMetadataManager, TierTopicManager, TierUtils}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import kafka.tier.archiver.JavaFunctionConversions._

import scala.collection.JavaConverters._

class TierArchiverStateTest {
  val mockTime = new MockTime()
  val tierTopicName = "__tier_topic"
  val tierTopicNumPartitions: Short = 1
  val logDirs = new util.ArrayList(Collections.singleton(System.getProperty("java.io.tmpdir")))
  val tierMetadataManager = new TierMetadataManager(new MemoryTierPartitionStateFactory(),
    Some(new MockInMemoryTierObjectStore("myBucket")),
    new LogDirFailureChannel(1),
    true)
  val blockingTaskExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  @Test
  def testAwaitingLeaderResult(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val properties = new Properties()
    properties.put("tier.enable", "true")

    val tierObjectStore = new MockInMemoryTierObjectStore("bar")

    val logConfig = LogTest.createLogConfig(segmentBytes = 2048 * 5, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)

    val nextStage = BeforeLeader(log, tierTopicManager, tierObjectStore, topicPartition, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should advance to BeforeUpload", state.isInstanceOf[BeforeUpload])
        state
      }

    nextStage.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testAwaitingLeaderResultFenced(): Unit = {
    val log = mock(classOf[AbstractLog])
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")
    val topicPartition = new TopicPartition("foo", 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))

    BeforeLeader(log, tierTopicManager, tierObjectStore, topicPartition, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should be fenced", ex.getCause.isInstanceOf[TierArchiverFencedException])
        assertEquals(state, null)
        null
      }.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testAwaitingUpload(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val metadata = new TierObjectMetadata(
      new TopicPartition("foo", 0),
      0,
      0L,
      1,
      1L,
      0,
      0,
      1,
      true,
      true,
      1.asInstanceOf[Byte]
    )
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)
    when(log.localLogSegments(0L, 0L)).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(OptionalLong.of(1))

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(metadata))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val nextStage = AfterUpload(
      metadata, mock(classOf[LogSegment]), log, tierTopicManager, tierObjectStore,
      topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig()
    ).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
      result
    }

    nextStage.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadFenced(): Unit = {
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)

    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(OptionalLong.empty())
    when(tierPartitionState.tierEpoch).thenReturn(1)

    val nextStage = BeforeUpload(log, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should be fenced", ex.isInstanceOf[TierArchiverFencedException])
        state
      }

    nextStage.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadRetryWhenNoSegment(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")

    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)
    when(log.tierableLogSegments).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(OptionalLong.empty())
    when(tierPartitionState.tierEpoch).thenReturn(0)

    val nextStage = BeforeUpload(
      log, tierTopicManager, tierObjectStore,
      topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig()
    ).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
      result
    }

    nextStage.get(2000, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadAdvancesToNextState(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val logConfig = LogTest.createLogConfig(segmentBytes =  150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)

    // setup mocks
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")
    val tierMetadataManager = mock(classOf[TierMetadataManager])
    val tierPartitionState = mock(classOf[TierPartitionState])
    val tierTopicManager = mock(classOf[TierTopicManager])

    // mock tierPartitionState state to emulate we are the leader at epoch 0
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.endOffset).thenReturn(OptionalLong.empty)

    // mock tierPartitionState so it returns the segments we have tiered so far
    val tieredSegments = new ConcurrentSkipListSet[java.lang.Long]()
    when(tierPartitionState.segmentOffsets).thenReturn(tieredSegments)

    // mock tierMetadataManager to return the corresponding partition state
    when(tierMetadataManager.initState(topicPartition, logDir, logConfig)).thenReturn(tierPartitionState)
    when(tierMetadataManager.tierPartitionState(topicPartition)).thenReturn(Optional.of(tierPartitionState))

    val logDirFailureChannel = mock(classOf[LogDirFailureChannel])
    val log = MergedLog(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime,
      60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs, logDirFailureChannel, Some(tierMetadataManager))

    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))

    val expectedTierableSegments = log.localLogSegments(0, log.logEndOffset)
    assertTrue(expectedTierableSegments.size > 0)

    // make all segments upto logEndOffset tierable
    TierUtils.ensureTierable(log, log.logEndOffset, topicPartition)
    assertEquals(expectedTierableSegments.toList, log.tierableLogSegments)

    // transition from BeforeUpload so tierable segments are archived
    expectedTierableSegments.foreach { currentSegment =>
      val nextStage = BeforeUpload(log, tierTopicManager, tierObjectStore, topicPartition,
        tierMetadataManager.tierPartitionState(topicPartition).get, 0, blockingTaskExecutor, TierArchiverConfig())
        .nextState()
        .handle { (result: TierArchiverState, _: Throwable) =>
          assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
          result
        }
      nextStage.get(2000, TimeUnit.MILLISECONDS)

      tieredSegments.add(currentSegment.baseOffset)
      when(tierPartitionState.endOffset).thenReturn(OptionalLong.of(currentSegment.readNextOffset - 1))
    }

    // verify size and contents of the segments we tiered
    expectedTierableSegments.foreach { segment =>
      val segmentMetadata = TierArchiverState.createObjectMetadata(log.topicPartition, 0, segment, true)

      // verify contents of segment, indices and state in tiered store
      assertTrue(isEqual(segment.log.file, tierObjectStore, segmentMetadata, TierObjectStoreFileType.SEGMENT))
      assertTrue(isEqual(segment.offsetIndex.file, tierObjectStore, segmentMetadata, TierObjectStoreFileType.OFFSET_INDEX))
      assertTrue(isEqual(segment.timeIndex.file, tierObjectStore, segmentMetadata, TierObjectStoreFileType.TIMESTAMP_INDEX))
      assertTrue(isEqual(log.leaderEpochCache.get.file, tierObjectStore, segmentMetadata, TierObjectStoreFileType.EPOCH_STATE))
    }

    log.close()
    Utils.delete(logDir)
  }

  @Test
  def testArchiverStateRelativePriority(): Unit = {
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")
    val topicPartition = new TopicPartition("foo", 0)
    val tierEpoch = 1
    val config = TierArchiverConfig()

    val beforeLeader = BeforeLeader(
      mockLogAtHighwatermark(1000L),
      tierTopicManager,
      tierObjectStore,
      topicPartition,
      tierEpoch,
      blockingTaskExecutor,
      config
    )

    val beforeUploadA = BeforeUpload(
      mockLogAtHighwatermark(1000L),
      tierTopicManager,
      tierObjectStore,
      topicPartition,
      mockTierPartitionStateAtEndOffset(0L),
      tierEpoch,
      blockingTaskExecutor,
      config
    )

    val beforeUploadB = BeforeUpload(
      mockLogAtHighwatermark(1000L),
      tierTopicManager,
      tierObjectStore,
      topicPartition,
      mockTierPartitionStateAtEndOffset(0L),
      tierEpoch,
      blockingTaskExecutor,
      config
    )

    val beforeUploadC = BeforeUpload(
      mockLogAtHighwatermark(1000L),
      tierTopicManager,
      tierObjectStore,
      topicPartition,
      mockTierPartitionStateAtEndOffset(900L),
      tierEpoch,
      blockingTaskExecutor,
      config
    )

    val afterUpload = AfterUpload(
      mock(classOf[TierObjectMetadata]),
      mock(classOf[LogSegment]),
      mockLogAtHighwatermark(1000L),
      tierTopicManager,
      tierObjectStore,
      topicPartition,
      mockTierPartitionStateAtEndOffset(0L),
      tierEpoch,
      blockingTaskExecutor,
      config
    )

    assertEquals("BeforeLeader states have greater priority than AfterUpload states",
      Priority.Higher, beforeLeader.relativePriority(afterUpload))

    assertEquals("AfterUpload states have greater priority than BeforeUpload states",
      Priority.Higher, afterUpload.relativePriority(beforeUploadA))

    assertEquals("BeforeUpload states with equal lag have same priority",
      Priority.Same, beforeUploadA.relativePriority(beforeUploadB))

    assertEquals("BeforeUpload states with greater lag have higher priority",
      Priority.Higher, beforeUploadA.relativePriority(beforeUploadC))
  }

  private def mockLogAtHighwatermark(hwm: Long): AbstractLog = {
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(Some(hwm))
    log
  }

  private def mockTierPartitionStateAtEndOffset(offset: Long): TierPartitionState = {
    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(OptionalLong.of(offset))
    tierPartitionState
  }

  private def isEqual(localFile: File,
                      tierObjectStore: TierObjectStore,
                      metadata: TierObjectMetadata,
                      fileType: TierObjectStoreFileType): Boolean = {
    val localStream = new FileInputStream(localFile.getPath)
    val tieredObject = tierObjectStore.getObject(metadata, fileType, 0, Integer.MAX_VALUE)

    val localData = ByteBuffer.allocate(localFile.length.toInt)
    val tieredData = ByteBuffer.allocate(tieredObject.getObjectSize.toInt)

    try {
      Utils.readFully(localStream, localData)
      Utils.readFully(tieredObject.getInputStream, tieredData)
    } finally {
      localStream.close()
      tieredObject.close()
    }
    localData.flip()
    tieredData.flip()

    localData.equals(tieredData)
  }
}
