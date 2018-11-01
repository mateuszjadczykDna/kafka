// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

public class Utils {

  private Utils() {
    // static class; can't instantiate
  }

  public static final String EMPTY_STRING = "";

  public static String notNullOrEmpty(String value) {
    return value == null ? EMPTY_STRING : value;
  }
}
