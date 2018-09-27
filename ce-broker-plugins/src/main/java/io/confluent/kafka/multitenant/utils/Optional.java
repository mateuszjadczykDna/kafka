// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.utils;

public class Optional<T> {
  private static final Optional<Object> NONE = new Optional<>(null);

  private final T value;

  private Optional(T value) {
    this.value = value;
  }

  @SuppressWarnings("unchecked")
  public static <T> Optional<T> none() {
    return (Optional<T>) NONE;
  }

  public static <T> Optional<T> some(T value) {
    if (value == null) {
      throw new IllegalArgumentException("Value must not be null");
    }
    return new Optional<>(value);
  }

  public boolean isDefined() {
    return value != null;
  }

  public T get() {
    if (value == null) {
      throw new IllegalStateException("No value present");
    }
    return value;
  }
}
