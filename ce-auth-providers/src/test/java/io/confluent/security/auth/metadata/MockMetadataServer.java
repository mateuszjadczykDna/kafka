// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.kafka.security.authorizer.Authorizer;
import java.io.IOException;
import java.util.Map;

public class MockMetadataServer implements MetadataServer {
  public enum ServerState {
    CREATED,
    CONFIGURED,
    STARTED,
    CLOSED
  }

  volatile ServerState serverState;
  volatile Authorizer embeddedAuthorizer;
  volatile AuthStore authStore;
  volatile AuthCache authCache;
  volatile Map<String, ?> configs;

  public MockMetadataServer() {
    this.serverState = ServerState.CREATED;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (serverState != ServerState.CREATED)
      throw new IllegalStateException("configure() invoked in invalid state: " + serverState);
    this.configs = configs;
    serverState = ServerState.CONFIGURED;
  }

  @Override
  public void start(Authorizer embeddedAuthorizer, AuthStore authStore) {
    if (serverState != ServerState.CONFIGURED)
      throw new IllegalStateException("start() invoked in invalid state: " + serverState);

    this.embeddedAuthorizer = embeddedAuthorizer;
    this.authStore = authStore;
    this.authCache = authStore.authCache();
    serverState = ServerState.STARTED;
  }

  @Override
  public void close() throws IOException {
    serverState = ServerState.CLOSED;
  }

  @Override
  public String providerName() {
    return "MOCK_RBAC";
  }
}
