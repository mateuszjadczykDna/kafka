// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.clients;

import io.confluent.security.auth.store.AuthCache;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.Scope;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Time;

public class KafkaAuthStore implements Configurable, AutoCloseable {

  private final AuthCache authCache;
  private final Time time;

  public KafkaAuthStore(Scope scope) {
    this(RbacRoles.loadDefaultPolicy(), Time.SYSTEM, scope);
  }

  public KafkaAuthStore(RbacRoles rbacRoles, Time time, Scope scope) {
    this.authCache = new AuthCache(rbacRoles, scope);
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  public void startReader() {
  }

  public AuthCache authCache() {
    return authCache;
  }

  @Override
  public void close() {
  }
}