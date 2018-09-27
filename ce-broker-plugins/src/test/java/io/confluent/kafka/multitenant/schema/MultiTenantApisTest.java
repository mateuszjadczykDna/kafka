// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.schema;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.Test;

public class MultiTenantApisTest {

  @Test
  public void testIsApiAllowedHandlesAllApiVersions() {
    // verify that we are handle all APIs. This ensures that the build will fail when a new
    // API is added to Kafka

    for (ApiKeys api : ApiKeys.values()) {
      MultiTenantApis.isApiAllowed(api);
    }
  }

}