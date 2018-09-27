/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

/**
 * The clock interface to get current system time
 */
public interface Clock {

  /**
   * Returns the current time in milliseconds.
   */
  long currentTimeMillis();
}
