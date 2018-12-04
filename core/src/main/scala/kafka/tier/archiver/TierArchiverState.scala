/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.nio.channels.FileChannel
import java.util.concurrent._
import java.util.function
import java.util.function.Supplier

import kafka.log.{Log, LogSegment}
import kafka.tier.TierTopicManager
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.archiver.CompletableFutureExtensions._
import kafka.tier.archiver.JavaFunctionConversions._
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetryableException}
import kafka.tier.serdes.State
import kafka.tier.state.TierPartitionState
import kafka.tier.store.TierObjectStore
import org.apache.kafka.common.TopicPartition

import scala.util.Random

sealed trait TierArchiverState {
  val topicPartition: TopicPartition

  def relativePriority(other: TierArchiverState): Int

  def nextState(): CompletableFuture[TierArchiverState]

  override def toString: String = {
    s"${getClass.getSimpleName}topicPartition=$topicPartition)"
  }
}

/*
TierArchiverState follows a status machine progression.
Each call to `nextState` can either successfully transition
to the next status or remain in the current status
(after a configurable retry timeout).
        +----------------+
        |                |
        |  BeforeLeader  |
        |                |
        +------+---------+
               |
               |
        +------v--------+
        |               |
        | BeforeUpload  <-----+
        |               |     |
        +------+--------+     |
               |              |
               |              |
        +------v-------+      |
        |              |      |
        | AfterUpload  +------+
        |              |
        +--------------+
 */

object TierArchiverState {

  object Priority extends Enumeration {
    val Higher: Int = -1
    val Lower: Int = 1
    val Same: Int = 0
  }

  // BeforeLeader represents a TopicPartition waiting for a successful fence TierTopic message
  // to go through. Once this has been realized by the TierTopicManager, it is allowed to progress
  // to BeforeUpload.
  final case class BeforeLeader(log: Log,
                                tierTopicManager: TierTopicManager,
                                tierObjectStore: TierObjectStore,
                                topicPartition: TopicPartition,
                                tierEpoch: Int,
                                blockingTaskExecutor: ScheduledExecutorService,
                                config: TierArchiverConfig) extends TierArchiverState {

    // Priority: BeforeLeader (this) > AfterUpload > BeforeUpload
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Same
        case _: BeforeUpload => Priority.Higher
        case _: AfterUpload => Priority.Higher
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      tierTopicManager
        .becomeArchiver(topicPartition, tierEpoch)
        .thenApply[TierArchiverState] { result: AppendResult =>
        result match {
          case AppendResult.ACCEPTED =>
            val tierPartitionState = tierTopicManager.getPartitionState(topicPartition)
            BeforeUpload(
              log, tierTopicManager, tierObjectStore,
              topicPartition, tierPartitionState, tierEpoch, blockingTaskExecutor, config
            )
          case AppendResult.ILLEGAL =>
            throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status.")
          case AppendResult.FENCED =>
            throw new TierArchiverFencedException(topicPartition)
        }
      }
    }
  }

  // BeforeUpload represents a TopicPartition checking for eligible segments to upload. If there
  // are no eligible we remain in the current status, if there are eligible segments we transition
  // to AfterUpload on completion of segment (and associated metadata) upload.
  final case class BeforeUpload(log: Log,
                                tierTopicManager: TierTopicManager,
                                tierObjectStore: TierObjectStore,
                                topicPartition: TopicPartition,
                                tierPartitionState: TierPartitionState,
                                tierEpoch: Int,
                                blockingTaskExecutor: ScheduledExecutorService,
                                config: TierArchiverConfig) extends TierArchiverState {

    def lag(): Long = highWatermark() - archivedOffset()

    // Priority: BeforeLeader > AfterUpload > BeforeUpload (this)
    // When comparing two BeforeUpload states, prioritize the state with greater lag higher.
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Lower
        case otherBeforeUpload: BeforeUpload => {
          val otherLag = otherBeforeUpload.lag()
          val thisLag = this.lag()
          if (otherLag > thisLag) {
            Priority.Lower
          } else if (otherLag < thisLag) {
            Priority.Higher
          } else {
            Priority.Same
          }
        }
        case _: AfterUpload => Priority.Lower
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      if (tierPartitionState.tierEpoch() != tierEpoch) {
        CompletableFutureUtil.failed(new TierArchiverFencedException(topicPartition))
      } else {
        log.logSegments(archivedOffset() + 1, highWatermark())
          .find(logSegment => logSegment != log.activeSegment) match {
          case None =>
            CompletableFutureUtil.completed(this)
          case Some(logSegment) =>
            putSegment(logSegment, blockingTaskExecutor)
              .thenApply[TierArchiverState] { objectMetadata: TierObjectMetadata =>
              AfterUpload(
                objectMetadata, logSegment, log, tierTopicManager, tierObjectStore,
                topicPartition, tierPartitionState, tierEpoch, blockingTaskExecutor, config
              )
            }
              .thenComposeExceptionally {
                case _: TierMetadataRetryableException => retryState()
                case ex: Exception => throw ex
              }
        }
      }
    }

    private def archivedOffset(): Long = tierPartitionState.endOffset().orElse(-1L)

    private def highWatermark(): Long = log.getHighWatermark.getOrElse(0L)

    private def putSegment(logSegment: LogSegment, blockingTaskExecutor: ScheduledExecutorService): CompletableFuture[TierObjectMetadata] = {
      CompletableFuture.supplyAsync(new Supplier[TierObjectMetadata] {
        override def get(): TierObjectMetadata = {
          val metadata = createObjectMetadata(logSegment)
          tierObjectStore.putSegment(metadata,
            FileChannel.open(logSegment.log.file().toPath),
            FileChannel.open(logSegment.offsetIndex.file.toPath),
            FileChannel.open(logSegment.timeIndex.file.toPath),
            FileChannel.open(logSegment.timeIndex.file.toPath), // FIXME producer status
            FileChannel.open(logSegment.timeIndex.file.toPath), // FIXME transaction index
            FileChannel.open(logSegment.timeIndex.file.toPath) // FIXME epoch status
          )
        }
      }, blockingTaskExecutor)
    }

    private def createObjectMetadata(logSegment: LogSegment): TierObjectMetadata = {
      val lastStableOffset = logSegment.readNextOffset - 1 // TODO: get from producer status snapshot
      val offsetDelta = lastStableOffset - logSegment.baseOffset
      new TierObjectMetadata(
        topicPartition.topic(),
        topicPartition.partition(),
        tierEpoch,
        logSegment.baseOffset,
        offsetDelta.intValue(),
        lastStableOffset,
        logSegment.largestTimestamp,
        logSegment.lastModified,
        logSegment.size,
        // TODO: compute whether any tx aborts occurred.
        false,
        State.AVAILABLE)
    }

    private val retryCount: Int = 0

    // Schedules the state to be scheduled again at some point in the future, specified by `backoffMs`.
    def retryState(): CompletableFuture[TierArchiverState] = {
      val future: CompletableFuture[TierArchiverState] = new CompletableFuture[TierArchiverState]()
      val self = this
      blockingTaskExecutor.schedule(new Callable[Unit] {
        override def call(): Unit = {
          future.complete(self)
        }
      }, backoffMs, TimeUnit.MILLISECONDS)
      future
    }

    // Calculate a random duration of seconds between zero and the current retry count
    private def backoffMs: Long = {
      if (retryCount == 0)
        0L
      else
        Math.min(
          config.maxRetryBackoffMs,
          Random.nextInt(retryCount) * 1000
        )
    }
  }

  // AfterUpload represents the TopicPartition writing out the TierObjectMetadata to the TierTopicManager,
  // after the TierTopicManager confirms that the TierObjectMetadata has been materialized, AfterUpload
  // transitions to BeforeUpload.
  final case class AfterUpload(objectMetadata: TierObjectMetadata,
                               logSegment: LogSegment,
                               log: Log,
                               tierTopicManager: TierTopicManager,
                               tierObjectStore: TierObjectStore,
                               topicPartition: TopicPartition,
                               tierPartitionState: TierPartitionState,
                               tierEpoch: Int,
                               blockingTaskExecutor: ScheduledExecutorService,
                               config: TierArchiverConfig) extends TierArchiverState {

    // Priority: BeforeLeader > AfterUpload (this) > BeforeUpload
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Lower
        case _: BeforeUpload => Priority.Higher
        case _: AfterUpload => Priority.Same
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      tierTopicManager.addMetadata(objectMetadata)
        .thenCompose(new function.Function[AppendResult, CompletableFuture[TierArchiverState]] {
            override def apply(t: AppendResult): CompletableFuture[TierArchiverState] = {
              t match {
                case AppendResult.ACCEPTED => {
                  CompletableFutureUtil.completed(
                    BeforeUpload(log,
                      tierTopicManager,
                      tierObjectStore,
                      topicPartition,
                      tierPartitionState,
                      tierEpoch,
                      blockingTaskExecutor,
                      config))
                }
                case AppendResult.ILLEGAL => {
                  throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status.")
                }
                case AppendResult.FENCED => {
                  throw new TierArchiverFencedException(topicPartition)
                }
              }
            }
          }
        )
    }
  }

}

