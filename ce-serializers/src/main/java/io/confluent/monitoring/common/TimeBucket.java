/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.monitoring.common;

import java.util.concurrent.TimeUnit;

public class TimeBucket {

  public static final long SIZE = TimeUnit.SECONDS.toMillis(15);
  public static final int HISTORY_SIZE = 10;
  public static final long SEQUENCE_TIMEOUT = 12 * SIZE;
  public static final long MAX_SESSION_DURATION = TimeUnit.MINUTES.toMillis(10);
}
