/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

object JavaFunctionConversions {
  implicit def function2JavaFunction[T, R](f: T => R): java.util.function.Function[T, R] =
    new java.util.function.Function[T, R] {
      override def apply(x: T): R = f(x)
    }

  implicit def function2JavaBiFunction[T1, T2, R](f: (T1, T2) => R): java.util.function.BiFunction[T1, T2, R] =
    new java.util.function.BiFunction[T1, T2, R] {
      override def apply(x: T1, y: T2): R = f(x, y)
    }
}
