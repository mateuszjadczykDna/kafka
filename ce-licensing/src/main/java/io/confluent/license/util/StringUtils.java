/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license.util;

/**
 * Simple string utilities, not present in java library, that are not worth importing a dependency.
 */
public class StringUtils {

  public static boolean isBlank(String string) {
    return string == null || string.isEmpty() || string.trim().isEmpty();
  }

  public static boolean isNotBlank(String string) {
    return !isBlank(string);
  }
}
