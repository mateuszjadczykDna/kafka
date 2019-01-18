/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import kafka.tier.domain.TierObjectMetadata

case class TierFetchMetadata(val fetchStartOffset: Long,
                             val maxOffset: Option[Long],
                             val maxBytes: Integer,
                             val maxPosition: Long,
                             val minOneMessage: Boolean,
                             val segmentMetadata: TierObjectMetadata,
                             val transactionMetadata: Option[List[TierObjectMetadata]],
                             val segmentBaseOffset: Long,
                             val segmentSize: Int)
