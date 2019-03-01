// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.provider;

import io.confluent.security.auth.client.RestClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UserInfoCredentialProviderTest {

  @Test
  public void testUserInfo() {
    Map<String, Object> clientConfig = new HashMap<>();
    clientConfig.put(RestClientConfig.BASIC_AUTH_USER_INFO_PROP, "user:password");
    UserInfoCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(clientConfig);
    Assert.assertEquals("user:password", provider.getUserInfo());
  }

  @Test(expected = ConfigException.class)
  public void testNullUserInfo() {
    Map<String, Object> clientConfig = new HashMap<>();
    UserInfoCredentialProvider provider = new UserInfoCredentialProvider();
    provider.configure(clientConfig);
  }

}
