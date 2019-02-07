// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.ldap;

import io.confluent.security.minikdc.MiniKdcWithLdapService;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityAuthentication;
import io.confluent.security.minikdc.MiniKdcWithLdapService.LdapSecurityProtocol;
import io.confluent.security.test.utils.LdapTestUtils;

public class LdapGssapiGroupEndToEndAuthorizationTest extends GroupEndToEndAuthorizationTest {

  @Override
  protected MiniKdcWithLdapService createLdapServer() throws Exception {
    return LdapTestUtils.createMiniKdcWithLdapService(LdapSecurityProtocol.PLAINTEXT,
        LdapSecurityAuthentication.GSSAPI);
  }
}

