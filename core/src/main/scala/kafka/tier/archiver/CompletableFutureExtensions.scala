/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.concurrent.CompletableFuture

import kafka.tier.archiver.JavaFunctionConversions._

object CompletableFutureExtensions {
  implicit class FlatMapExceptionsLikeRecoverWith[T](completableFuture: CompletableFuture[T]) {
    def thenComposeExceptionally[U >: T](f: Throwable => CompletableFuture[U]): CompletableFuture[U] = {
      completableFuture.handle[CompletableFuture[U]] { (result: T, ex: Throwable) =>
        if (ex != null)
          f(ex)
        else
          CompletableFutureUtil.completed[U](result)
      }.thenCompose { identity: CompletableFuture[U] => identity }
    }
  }
}
