// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.kafka.security.authorizer.Authorizer;
import java.io.Closeable;
import org.apache.kafka.common.Configurable;

public interface MetadataServer extends Configurable, Closeable {

  /**
   * Starts metadata server. This method should return as soon as possible after initiating
   * server start since broker's startup is blocked until this method returns.
   *
   * @param embeddedAuthorizer An embedded cross-component authorizer that can be used to authorize any action
   * @param authStore An instance of AuthStore that can be used to read current role assignments,
   *                 user metadata etc. from an in-memory cache that is kept up-to-date by the embedded
   *                 authorizer. AuthStore can also be used to update role assignments. The cache of
   *                 `authStore` is initialized before this method is invoked.
   */
  void start(Authorizer embeddedAuthorizer, AuthStore authStore);

}
