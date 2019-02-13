// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.clients;

import io.confluent.security.auth.metadata.AuthListener;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.store.KafkaAuthCache;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.Scope;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Time;

public class KafkaAuthStore implements AuthStore {

  private final KafkaAuthCache authCache;
  private final Time time;
  private final Set<AuthListener> listeners;

  public KafkaAuthStore(Scope scope) {
    this(RbacRoles.loadDefaultPolicy(), Time.SYSTEM, scope);
  }

  public KafkaAuthStore(RbacRoles rbacRoles, Time time, Scope scope) {
    this.authCache = new KafkaAuthCache(rbacRoles, scope);
    this.time = time;
    this.listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());
    addListener(authCache);
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public void start() {
  }

  @Override
  public KafkaAuthCache authCache() {
    return authCache;
  }

  @Override
  public boolean addListener(AuthListener listener) {
    return listeners.add(listener);
  }

  @Override
  public boolean removeListener(AuthListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public void close() {
  }

  // Visibility for testing
  Collection<AuthListener> listeners() {
    return Collections.unmodifiableSet(listeners);
  }
}