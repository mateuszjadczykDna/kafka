package kafka.raft

import java.lang
import java.util.Optional

import kafka.log.Log
import kafka.server.OffsetAndEpoch
import org.apache.kafka.common.{KafkaException, raft}
import org.apache.kafka.common.raft.ReplicatedLog
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.Time

class KafkaMetadataLog(time: Time, log: Log, maxFetchSizeInBytes: Int = 1024 * 1024) extends ReplicatedLog {

  override def read(startOffset: Long, highWatermark: Long): Records = {
    val fetchInfo = log.read(startOffset,
      maxLength = maxFetchSizeInBytes,
      maxOffset = Some(highWatermark),
      minOneMessage = true,
      includeAbortedTxns = false)
    fetchInfo.records
  }

  override def appendAsLeader(records: Records, epoch: Int): lang.Long = {
    val appendInfo = log.appendAsLeader(records.asInstanceOf[MemoryRecords], leaderEpoch = epoch)
    appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }
  }

  override def appendAsFollower(records: Records): Unit = {
    log.appendAsFollower(records.asInstanceOf[MemoryRecords])
  }

  override def latestEpoch: Int = {
    log.latestEpoch.getOrElse(0)
  }

  override def endOffsetForEpoch(leaderEpoch: Int): Optional[raft.OffsetAndEpoch] = {
    // TODO: Does this handle empty log case (when epoch is None) as we expect?
    val endOffsetOpt = log.endOffsetForEpoch(leaderEpoch).map { offsetAndEpoch =>
      new raft.OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)
    }
    convert(endOffsetOpt)
  }

  private def convert[T](opt: Option[T]): Optional[T] = {
    opt match {
      case Some(thing) => Optional.of(thing)
      case None => Optional.empty()
    }
  }

  override def endOffset: Long = {
    log.logEndOffset
  }

  override def startOffset: Long = {
    log.localLogStartOffset
  }

  override def truncateTo(offset: Long): Boolean = {
    log.truncateTo(offset)
  }

  override def previousEpoch: Optional[Integer] = {
    convert(log.previousEpoch.map(_.asInstanceOf[Integer]))
  }

  override def assignEpochStartOffset(epoch: Int, startOffset: Long): Unit = {
    log.maybeAssignEpochStartOffset(epoch, startOffset)
  }

}
