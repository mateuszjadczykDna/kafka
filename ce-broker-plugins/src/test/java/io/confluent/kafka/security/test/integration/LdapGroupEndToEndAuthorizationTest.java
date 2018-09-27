// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.integration;

import io.confluent.kafka.security.minikdc.MiniKdcWithLdapService;
import io.confluent.kafka.security.test.utils.LdapTestUtils;

public class LdapGroupEndToEndAuthorizationTest extends GroupEndToEndAuthorizationTest {

  @Override
  protected MiniKdcWithLdapService createLdapServer() throws Exception {
    return LdapTestUtils.createMiniKdcWithLdapService(null, null);
  }
}

