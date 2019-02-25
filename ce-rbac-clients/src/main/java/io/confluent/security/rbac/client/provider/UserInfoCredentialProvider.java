// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.provider;


import io.confluent.security.rbac.client.RbacRestClientConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class UserInfoCredentialProvider implements BasicAuthCredentialProvider {

  private String userInfo;

  @Override
  public String providerName() {
    return "USER_INFO";
  }

  @Override
  public void configure(Map<String, ?> configs) {
    userInfo = (String) configs.get(RbacRestClientConfig.BASIC_AUTH_USER_INFO_PROP);
    if (userInfo != null && !userInfo.isEmpty()) {
      return;
    }

    throw new ConfigException(RbacRestClientConfig.BASIC_AUTH_USER_INFO_PROP + " must be provided when " +
            RbacRestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP + " is set to " +
            BuiltInAuthProviders.BasicAuthCredentialProviders.USER_INFO.name());
  }

  @Override
  public String getUserInfo() {
    return userInfo;
  }
}
