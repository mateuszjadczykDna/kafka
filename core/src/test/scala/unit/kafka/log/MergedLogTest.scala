/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.server.{BrokerTopicStats, FetchDataInfo, KafkaConfig, LogDirFailureChannel, TierFetchDataInfo}
import kafka.tier.TierMetadataManager
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.MockInMemoryTierObjectStore
import kafka.utils.{MockTime, Scheduler, TestUtils}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.Assert.{assertEquals, fail}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class MergedLogTest {
  val brokerTopicStats = new BrokerTopicStats
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val mockTime = new MockTime()
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Some(new MockInMemoryTierObjectStore("myBucket")),
    new LogDirFailureChannel(1),
    true)
  val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)

  @After
  def tearDown() {
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testReadFromTieredRegion(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val tierStart = ranges.firstTieredOffset
    val tierEnd = ranges.firstOverlapOffset.get - 1
    val offsetsToRead = List(tierStart, tierStart + 1, tierEnd - 1, tierEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case tierResult: TierFetchDataInfo => {
          val segmentBaseOffset = tierPartitionState.segmentOffsets.floor(offset)
          assertEquals(segmentBaseOffset, tierResult.fetchMetadata.segmentBaseOffset)
        }
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }


  @Test
  def testCannotUploadPastRecoveryPoint(): Unit = {

    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): Unit = ()
    }
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createMergedLog(logConfig, scheduler = noopScheduler)
    val messagesToWrite = 10
    for (_ <- 0 until messagesToWrite) {
      val segmentStr = "foo"
      val messageStr = "bar"
      def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
      log.appendAsLeader(createRecords, 0)
      log.roll()
    }

    assertEquals("Each message should create a single log segment", log.localLogSegments.size, 11)
    // Set the high watermark to the active segments end offset and flush up to the 4th segment
    log.onHighWatermarkIncremented(log.localLog.activeSegment.readNextOffset - 1)
    log.flush(4) // flushes up to 3, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment - 1 segment",
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector, Vector(0, 1, 2))

    log.flush(8) // flushes up to 7, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment - 1 segment",
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector, Vector(0, 1, 2, 3, 4, 5, 6))

  }

  @Test
  def testReadFromOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val overlapStart = ranges.firstOverlapOffset.get
    val overlapEnd = ranges.lastOverlapOffset.get
    val offsetsToRead = List(overlapStart, overlapStart + 1, overlapEnd - 1, overlapEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testReadAboveOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val localStart = ranges.lastOverlapOffset.get + 1
    val localEnd = ranges.lastLocalOffset - 1
    val offsetsToRead = List(localStart, localStart + 1, localEnd - 1, localEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testIncrementLogStartOffset(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)
    val offsets = List(ranges.firstOverlapOffset.get - 1,
      ranges.firstOverlapOffset.get,
      ranges.firstOverlapOffset.get + 1,
      ranges.lastOverlapOffset.get - 1,
      ranges.lastOverlapOffset.get,
      ranges.lastOverlapOffset.get + 1,
      log.activeSegment.baseOffset - 1,
      log.logEndOffset)
    val epochs =
      for (i <- 0 until offsets.size)
        yield i

    val offsetToEpoch: ConcurrentNavigableMap[Long, Long] = new ConcurrentSkipListMap

    for ((offset, epoch) <- offsets zip epochs) {
      log.leaderEpochCache.get.assign(epoch, offset)
      offsetToEpoch.put(offset, epoch)
    }

    offsets.foreach { offset =>
      log.maybeIncrementLogStartOffset(offset)
      assertEquals(offset, log.logStartOffset)

      // validate epoch cache truncation
      val expectedEpochs = offsetToEpoch.tailMap(offset)
      val epochsInLog = log.leaderEpochCache.get.epochEntries
      assertEquals(expectedEpochs.keySet.asScala.toList, epochsInLog.map(_.startOffset).toList)
      assertEquals(expectedEpochs.values.asScala.toList, epochsInLog.map(_.epoch).toList)
    }
    log.close()
  }

  @Test
  def testRetentionOnLocalSegments(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)
    val epoch = 0
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get

    // tier couple of segments
    val tierEndOffset = tierPartitionState.endOffset.getAsLong
    val segmentsToTier = log.localLogSegments(tierEndOffset + 1, Long.MaxValue).take(2)
    segmentsToTier.foreach { segment =>
      val tierObjectMetadata = new TierObjectMetadata(log.topicPartition,
        epoch,
        segment.baseOffset,
        (segment.readNextOffset - segment.baseOffset - 1).toInt,
        segment.readNextOffset,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        true,
        false,
        0.toByte)
      val appendResult = tierPartitionState.append(tierObjectMetadata)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    val numOverlapSegments = log.localLogSegments(ranges.firstOverlapOffset.get, ranges.lastOverlapOffset.get).size
    val numDeleted = log.deleteOldSegments()
    assertEquals(numOverlapSegments + segmentsToTier.size, numDeleted)
    assertEquals(ranges.firstTieredOffset, log.logStartOffset)
    assertEquals(log.localLogSegments.head.baseOffset, segmentsToTier.last.readNextOffset)

    // read from one of the tiered segments
    val result = log.read(tierEndOffset + 1, Int.MaxValue, None, true, false)
    result match {
      case tierResult: TierFetchDataInfo => assertEquals(tierEndOffset + 1, tierResult.fetchMetadata.segmentBaseOffset)
      case _ => fail(s"Unexpected $result")
    }
  }

  @Test
  def testSizeOfLogWithOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionBytes = 1, segmentBytes = Int.MaxValue, tierEnable = true)
    val numTieredSegments = 30
    val numLocalSegments = 50
    val numOverlapSegments = 10
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlapSegments, logConfig)

    val segmentSize = log.localLogSegments.head.size
    val expectedLogSize = ((numTieredSegments - numOverlapSegments) + (numLocalSegments + numOverlapSegments - 1)) * segmentSize
    assertEquals(expectedLogSize, log.size)
  }

  private def logRanges(log: MergedLog): LogRanges = {
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get

    val firstTieredOffset = log.logStartOffset
    val lastTieredOffset = tierPartitionState.metadata(tierPartitionState.segmentOffsets().last()).get.endOffset
    val firstLocalOffset = log.localLogSegments.head.baseOffset
    val lastLocalOffset = log.logEndOffset

    if (firstLocalOffset <= lastTieredOffset)
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, Some(firstLocalOffset), Some(lastTieredOffset))
    else
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, None, None)
  }

  private def createLogWithOverlap(numTieredSegments: Int, numLocalSegments: Int, numOverlap: Int, logConfig: LogConfig): MergedLog = {
    var log = createMergedLog(logConfig)
    var tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    tierPartitionState.onCatchUpComplete()

    val messagesPerSegment = 20
    val epoch = 0

    // create all segments as local initially
    for (segment <- 0 until (numTieredSegments + numLocalSegments - 1)) {
      for (message <- 0 until messagesPerSegment) {
        val segmentStr = "%06d".format(segment)
        val messageStr = "%06d".format(message)
        def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
        log.appendAsLeader(createRecords, leaderEpoch = 0)
      }
      log.roll(None)
    }
    log.onHighWatermarkIncremented(log.logEndOffset)
    assertEquals(numTieredSegments + numLocalSegments, log.localLogSegments.size)

    // append an init message
    tierPartitionState.append(new TierTopicInitLeader(log.topicPartition,
      epoch,
      java.util.UUID.randomUUID(),
      0))

    // initialize metadata for tiered segments
    val segmentsToTier = log.localLogSegments.take(numTieredSegments)
    segmentsToTier.foreach { segment =>
      val tierObjectMetadata = new TierObjectMetadata(log.topicPartition,
        epoch,
        segment.baseOffset,
        (segment.readNextOffset - segment.baseOffset - 1).toInt,
        segment.readNextOffset,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        true,
        false,
        0.toByte)
      val appendResult = tierPartitionState.append(tierObjectMetadata)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    val localSegmentsToDelete = segmentsToTier.take(numTieredSegments - numOverlap)
    log.localLog.deleteOldSegments(Some(localSegmentsToDelete.last.readNextOffset), log.size)

    // close the log
    log.close()
    tierMetadataManager.close()

    // reopen
    log = createMergedLog(logConfig)
    log.onHighWatermarkIncremented(log.logEndOffset)
    tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    tierPartitionState.onCatchUpComplete()

    // assert number of segments
    assertEquals(numLocalSegments + numOverlap, log.localLogSegments.size)
    log.uniqueLogSegments match {
      case (tierLogSegments, localLogSegments) =>
        assertEquals(numTieredSegments - numOverlap, tierLogSegments.size)
        assertEquals(numLocalSegments + numOverlap, localLogSegments.size)
        assertEquals(numTieredSegments, tierPartitionState.segmentOffsets.size)
    }
    log
  }

  private def createMergedLog(config: LogConfig,
                              brokerTopicStats: BrokerTopicStats = brokerTopicStats,
                              logStartOffset: Long = 0L,
                              recoveryPoint: Long = 0L,
                              scheduler: Scheduler = mockTime.scheduler,
                              time: Time = mockTime,
                              maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                              producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs): MergedLog = {
    MergedLog(dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      tierMetadataManagerOpt = Some(tierMetadataManager))
  }

  private case class LogRanges(val firstTieredOffset: Long,
                               val lastTieredOffset: Long,
                               val firstLocalOffset: Long,
                               val lastLocalOffset: Long,
                               val firstOverlapOffset: Option[Long],
                               val lastOverlapOffset: Option[Long])
}
