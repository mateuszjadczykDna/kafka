/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.tier.store.TierObjectStore
import kafka.tier.fetcher.TierFetchMetadata
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction

sealed trait FetchIsolation
case object FetchLogEnd extends FetchIsolation
case object FetchHighWatermark extends FetchIsolation
case object FetchTxnCommitted extends FetchIsolation

sealed trait AbstractFetchDataInfo {
  def addAbortedTransactions(abortedTransactions: List[AbortedTransaction]): AbstractFetchDataInfo
  def abortedTransactions: Option[List[AbortedTransaction]]
  def records: Records
}

case class FetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                         records: Records,
                         firstEntryIncomplete: Boolean = false,
                         abortedTransactions: Option[List[AbortedTransaction]] = None) extends AbstractFetchDataInfo {
  override def addAbortedTransactions(abortedTransactions: List[AbortedTransaction]): FetchDataInfo = {
    copy(abortedTransactions = Some(abortedTransactions))
  }
}

/**
  * Contains both the metadata required to complete a fetch from Tiered Storage, and the results of that fetch.
  * Note that when this object is returned from the Log layer, it will have empty records. It is up to `DelayedFetch`
  * to coordinate with the TierFetcher in order to fill in the record data.
  */
case class TierFetchDataInfo(fetchMetadata: TierFetchMetadata,
                             records: Records,
                             tierObjectStore: TierObjectStore,
                             abortedTransactions: Option[List[AbortedTransaction]] = None) extends AbstractFetchDataInfo {
  override def addAbortedTransactions(localAbortedTransactions: List[AbortedTransaction]): TierFetchDataInfo = {
    copy(abortedTransactions = Some(localAbortedTransactions))
  }
}
