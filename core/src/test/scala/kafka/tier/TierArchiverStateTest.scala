/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, OptionalLong, Properties}

import kafka.log._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.archiver.JavaFunctionConversions._
import kafka.tier.archiver.TierArchiverState.{AfterUpload, BeforeLeader, BeforeUpload, Priority}
import kafka.tier.archiver._
import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.state.{MemoryTierPartitionStateFactory, TierPartitionState}
import kafka.tier.store.MockInMemoryTierObjectStore
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito.{mock, when}

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

    val tierTopicManagerConfig = new TierTopicManagerConfig(
      "bootstrap",
      null,
      1,
      1,
      33,
      "cluster99",
      200L,
      500,
      logDirs)


    val producerBuilder = new MockProducerBuilder()
    val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig, producerBuilder.producer())
    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      tierMetadataManager)

    val properties = new Properties()
    properties.put("tier.enable", "true")

    tierTopicManager.becomeReady()

    tierMetadataManager.initState(topicPartition, new File(logDirs.get(0)), new LogConfig(properties))
    tierMetadataManager.becomeLeader(topicPartition, 1)

    while(tierTopicManager.doWork()) {}

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

    consumerBuilder.moveRecordsFromProducer()
    tierTopicManager.doWork()

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
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore("bucket")

    val logConfig = LogTest.createLogConfig(segmentBytes =  150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(OptionalLong.empty())
    when(tierPartitionState.tierEpoch).thenReturn(0)

    log.appendAsFollower(createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(createRecords(5, topicPartition, log.logEndOffset, 0))
    log.onHighWatermarkIncremented(log.logEndOffset)

    val nextStage = BeforeUpload(
      log, tierTopicManager, tierObjectStore,
      topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig()
    ).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
      result
    }

    nextStage.get(2000, TimeUnit.MILLISECONDS)
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

  private def createRecords(n: Int, partition: TopicPartition, baseOffset: Long, leaderEpoch: Int): MemoryRecords = {
    val recList = Range(0, n).map(_ =>
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val records = TestUtils.records(records = recList, baseOffset = baseOffset)
    val filtered = ByteBuffer.allocate(100 * n)
    records.batches().asScala.foreach(_.setPartitionLeaderEpoch(leaderEpoch))
    records.filterTo(partition, new RecordFilter {
      override protected def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetention =
        RecordFilter.BatchRetention.DELETE_EMPTY
      override protected def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean =
        true
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    MemoryRecords.readableRecords(filtered)
  }

}
