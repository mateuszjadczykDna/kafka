/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.metrics;

public class Statistics {

  /**
   * Relative Mean Absolute Deviation
   *
   * @param values collection of values
   * @return the relative mean absolute deviation of the given values
   */
  public static double rmad(long[] values) {
    if (values.length == 0) {
      return 0;
    }
    double md = 0;
    double mean = 0;
    for (long yi : values) {
      mean += yi;
      for (long yj : values) {
        md += Math.abs(yi - yj);
      }
    }
    md /= (double) values.length * values.length;
    mean /= values.length;

    return mean != 0 ? md / mean : 0;
  }
}
