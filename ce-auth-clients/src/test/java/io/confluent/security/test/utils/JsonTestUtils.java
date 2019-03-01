// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import io.confluent.security.rbac.utils.JsonMapper;
import java.io.IOException;

public class JsonTestUtils {

  public static <T> T jsonObject(Class<T> clazz, String json) {
    try {
      return JsonMapper.objectMapper().readValue(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException("Invalid json: " + json, e);
    }
  }

  public static <T> T jsonArray(Class<T> clazz, String... jsonEntries) {
    StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("[ ");
    for (int i = 0; i < jsonEntries.length; i++) {
      if (i != 0)
        jsonBuilder.append(',');
      jsonBuilder.append(jsonEntries[i]);
    }
    jsonBuilder.append(" ]");
    return jsonObject(clazz, jsonBuilder.toString());
  }
}
