/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.metrics;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StatisticsTest {

  @Test
  public void testRelativeMeanAbsoluteDeviation() {
    assertEquals(0, Statistics.rmad(new long[]{}), 0);
    assertEquals(0, Statistics.rmad(new long[]{42}), 0);
    assertEquals(0, Statistics.rmad(new long[]{0}), 0);
    assertEquals(0, Statistics.rmad(new long[]{0, 0, 0}), 0);
    assertEquals(0, Statistics.rmad(new long[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}), 0);
    assertEquals(0, Statistics.rmad(new long[]{1, 1, 1, 1, 1, -1, -1, -1, -1, -1}), 0);
    assertEquals(1, Statistics.rmad(new long[]{1, 1, 1, 1, 1, 0, 0, 0, 0, 0}), 0);
    assertEquals(1, Statistics.rmad(new long[]{10, 10, 10, 10, 10, 0, 0, 0, 0, 0}), 0);
    assertEquals(0.5, Statistics.rmad(new long[]{3, 3, 3, 1, 1, 1}), 0);
    assertEquals(0.5, Statistics.rmad(new long[]{6, 6, 6, 2, 2, 2}), 0);
    assertEquals(-0.5, Statistics.rmad(new long[]{-6, -6, -6, -2, -2, -2}), 0);
    assertEquals(8.0 / 9.0, Statistics.rmad(new long[]{0, 10, 20}), 1e-7);
  }
}
