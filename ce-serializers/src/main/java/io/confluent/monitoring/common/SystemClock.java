/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

public class SystemClock implements Clock {

  /**
   * Returns the current time in milliseconds.
   */
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }

}
