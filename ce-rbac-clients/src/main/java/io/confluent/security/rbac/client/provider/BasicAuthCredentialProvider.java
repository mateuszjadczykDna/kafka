// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.client.provider;

import org.apache.kafka.common.Configurable;


/**
 * Interface used by providers user credentials for HTTP basic authentication
 */
public interface BasicAuthCredentialProvider extends Configurable {

  /**
   * Returns the name of this provider.
   * @return provider name
   */
  String providerName();

  /**
   * Returns user credentials
   * @return user credentials
   */
  String getUserInfo();
}
