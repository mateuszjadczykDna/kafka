/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util

import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class TierPartitionStateEntryTest {
  @Test
  def serializeDeserializeTest(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val n = 200
    val epoch = 0
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

    state.beginCatchup()
    state.onCatchUpComplete()
    val path = state.path
    try {
      state.append(
        new TierTopicInitLeader(
          tp,
          epoch,
          java.util.UUID.randomUUID(),
          0))
      var size = 0
      for (i <- 0 until n) {
        state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, 0))
        size += i
      }

      val segmentOffsets = state.segmentOffsets.iterator

      for (i <- 0 until n) {
        val startOffset = i * 2L
        assertEquals(startOffset, segmentOffsets.next)
        assertEquals(startOffset, state.metadata(startOffset).get().startOffset())
      }
      assertFalse(segmentOffsets.hasNext)

      assertEquals(n, state.numSegments())
      assertEquals(size, state.totalSize)
      assertEquals(0L, state.startOffset().get())
      assertEquals(n * 2 - 1 : Long, state.endOffset().get())

      state.close()

      checkInsufficientPayloadTruncated(dir, tp, path)
      checkInsufficientSizeHeaderTruncated(dir, tp, path)
    }
    finally {
      dir.delete()
    }
  }

  @Test
  def overlappingOffsetRangesTest(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

    state.beginCatchup()
    state.onCatchUpComplete()
    val path = state.path
    try {
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tp, 0, java.util.UUID.randomUUID(), 0)))
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 0, 100, 100, 0, 0, false, false, 0)))
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 100, 100, 200, 0, 0, false, false, 0)))
      // leader failover
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tp, 1, java.util.UUID.randomUUID(), 0)))
      // should be fenced because segment has no new data

      assertEquals(TierPartitionState.AppendResult.FENCED, state.append(new TierObjectMetadata(tp, 1, 190, 10, 200, 0, 0, false, false, 0)))
      // should be accepted because segment overlaps but has additional data
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 1, 180, 100, 200, 0, 0, false, false, 0)))

      state.close()
    }
    finally {
      dir.delete()
    }
  }


  @Test
  def checkPartiallyWrittenFilePartiallyReadable() = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val n = 10
    val epoch = 0
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

    state.beginCatchup()
    state.onCatchUpComplete()
    val channel = state.asInstanceOf[FileTierPartitionState].channel()

    val positions = new util.TreeMap[Long,Long]()

    try {
      state.append(
        new TierTopicInitLeader(
          tp,
          epoch,
          java.util.UUID.randomUUID(),
          0))
      var size = 0

      positions.put(0, 0)
      for (i <- 0 until n) {
        state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, 0))
        size += i
        positions.put(channel.size, size)
      }

      // simulate reading a partially written file
      while (state.asInstanceOf[FileTierPartitionState].channel().size() > 0) {
        channel.truncate(channel.size() - 1)
        assertEquals(positions.floorEntry(channel.size()).getValue, state.totalSize())
      }
      assertEquals(0, state.totalSize())
    } finally {
      dir.delete()
    }
  }

  private def checkInsufficientSizeHeaderTruncated(baseDir: File, tp: TopicPartition, path: String) = {
    // write some garbage to the end to test truncation
    val channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val origSize = channel.size()
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1: Byte)
    buf.flip()
    channel.position(channel.size())
    channel.write(buf)
    channel.close()

    // re-open to force truncate
    val state2 = new FileTierPartitionState(baseDir, tp, true)
    state2.beginCatchup()
    state2.onCatchUpComplete()
    state2.close()
    val channel2 = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val newSize = channel2.size()

    // check size before garbage written = size after truncation
    assertEquals("insufficient size header, not truncated", origSize, newSize)
  }

  private def checkInsufficientPayloadTruncated(baseDir: File, tp: TopicPartition, path: String) = {
    // write some garbage to the end to test truncation
    val channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val origSize = channel.size()
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(80)
    buf.putInt(1)
    buf.flip()
    channel.position(channel.size())
    channel.write(buf)
    channel.close()

    // re-open to force truncate
    val state2 = new FileTierPartitionState(baseDir, tp, true)
    state2.beginCatchup()
    state2.onCatchUpComplete()
    state2.close()
    val channel2 = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val newSize = channel2.size()

    // check size before garbage written = size after truncation
    assertEquals("valid size header, insufficient payload header not truncated", origSize, newSize)
  }
}
