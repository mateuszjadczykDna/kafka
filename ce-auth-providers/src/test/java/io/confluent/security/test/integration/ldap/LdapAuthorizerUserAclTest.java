/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.security.test.integration.ldap;

import io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizerTest;
import io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.test.utils.LdapTestUtils;
import java.util.HashMap;
import java.util.Map;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.SimpleAclAuthorizerTest;
import kafka.server.KafkaConfig$;

// Note: This test has been very useful for early development and testing, especially
// as SimpleAclAuthorizer has been changing frequently in AK as wildcard support was
// adding in a series of conflicting PRs. Long-term, it would be very hard to maintain
// this test, so we should remove it once the code base is stable.
public class LdapAuthorizerUserAclTest extends ConfluentKafkaAuthorizerTest {

  private MiniKdcWithLdapService miniKdcWithLdapService;
  private LdapAuthorizer ldapAuthorizer;
  private LdapAuthorizer ldapAuthorizer2;

  private String ldapSuperUsers;

  @Override
  public void setUp() {
    super.setUp();

    ldapAuthorizer = new TestLdapAuthorizer();
    ldapAuthorizer2 = new TestLdapAuthorizer();
    initializeForLdapTest();

    try {
      miniKdcWithLdapService = LdapTestUtils.createMiniKdcWithLdapService(null, null);

      Map<String, Object> authorizerConfigs = new HashMap<>();
      authorizerConfigs.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect());
      authorizerConfigs.put(SimpleAclAuthorizer.SuperUsersProp(), ldapSuperUsers);
      ldapAuthorizer.configure(authorizerConfigs);
      ldapAuthorizer2.configure(authorizerConfigs);
    } catch (Exception e) {
      throw new RuntimeException("LDAP authorizer set up failed", e);
    }
  }

  @Override
  public void tearDown() {
    if (ldapAuthorizer != null) {
      ldapAuthorizer.close();
    }
    if (ldapAuthorizer2 != null) {
      ldapAuthorizer2.close();
    }
    if (miniKdcWithLdapService != null) {
      miniKdcWithLdapService.shutdown();
    }
  }

  // Some tests in SimpleAclAuthorizerTest configure the authorizer with just
  // broker configs, so make sure LDAP configs are added.
  private class TestLdapAuthorizer extends LdapAuthorizer {
    @Override
    public void configure(Map<String, ?> configs) {
      Map<String, Object> authorizerConfigs = new HashMap<>();
      authorizerConfigs.putAll(configs);
      authorizerConfigs.putAll(LdapTestUtils.ldapAuthorizerConfigs(miniKdcWithLdapService, 10));
      authorizerConfigs.putAll(miniKdcWithLdapService.ldapClientConfigs());
      super.configure(authorizerConfigs);
    }
  }

  private void initializeForLdapTest() {
    try {
      ldapSuperUsers = KafkaTestUtils.fieldValue(this, SimpleAclAuthorizerTest.class, "superUsers");
      KafkaTestUtils.setFinalField(this, SimpleAclAuthorizerTest.class,
          "simpleAclAuthorizer", simpleAclAuthorizer(ldapAuthorizer));
      KafkaTestUtils.setFinalField(this, SimpleAclAuthorizerTest.class,
          "simpleAclAuthorizer2", simpleAclAuthorizer(ldapAuthorizer2));
    } catch (Exception e) {
      throw new RuntimeException("Could not initialize test", e);
    }
  }
}

