/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import kafka.server.TierFetchDataInfo
import kafka.tier.TierFetchMetadata
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.store.TierObjectStore

class TierLogSegment private[log] (private val segment: TierObjectMetadata,
                                   private val tierObjectStore: TierObjectStore) {
  def baseOffset: Long = segment.startOffset

  def size: Int = segment.size

  def read(startOffset: Long,
           maxOffset: Option[Long],
           maxSize: Int,
           maxPosition: Long,
           minOneMessage: Boolean): Option[TierFetchDataInfo] = {
    if (startOffset < segment.startOffset() || startOffset > segment.endOffset()) {
      None
    } else {
      val maximumReadableBytes = math.min(maxSize, segment.size)
      val fetchMetadata = TierFetchMetadata(fetchStartOffset = startOffset,
        maxOffset = maxOffset,
        maxBytes = maximumReadableBytes,
        maxPosition = maxPosition,
        minOneMessage = minOneMessage,
        segment,
        transactionMetadata = None,
        segmentBaseOffset = baseOffset,
        segmentSize = segment.size)
      Some(TierFetchDataInfo(fetchMetadata, tierObjectStore))
    }
  }

  def nextOffset: Long = segment.endOffset + 1

  override def toString = s"baseOffset: $baseOffset tierObjectStore: $tierObjectStore"
}

