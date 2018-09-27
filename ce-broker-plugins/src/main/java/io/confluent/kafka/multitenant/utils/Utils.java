// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.utils;

public class Utils {

  public static String requireNonEmpty(String value, String argName) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(argName + " must not be empty or null");
    }
    return value;
  }

}
