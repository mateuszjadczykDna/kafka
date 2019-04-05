// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.external.ExternalStore;
import io.confluent.security.auth.store.external.ExternalStoreListener;
import io.confluent.security.auth.store.kafka.KafkaAuthWriter;
import io.confluent.security.rbac.UserMetadata;
import io.confluent.security.store.MetadataStoreStatus;
import java.util.Map;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;

public class LdapStore implements ExternalStore {

  private final Time time;
  private final UserStoreListener listener;
  private LdapConfig config;
  private LdapGroupManager ldapGroupManager;

  public LdapStore(AuthCache authCache, KafkaAuthWriter writer, Time time) {
    this.time = time;
    this.listener = new UserStoreListener(authCache, writer);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new LdapConfig(configs);
  }

  @Override
  public void start(int generationId) {
    listener.generationId = generationId;
    if (ldapGroupManager == null) {
      ldapGroupManager = new LdapGroupManager(config, time, listener);
      ldapGroupManager.start();
    }
  }

  @Override
  public void stop(Integer generationId) {
    if (ldapGroupManager != null)
      ldapGroupManager.close();
    listener.generationId = -1;
  }

  @Override
  public boolean failed() {
    LdapGroupManager manager = ldapGroupManager;
    return manager != null && manager.failed();
  }

  private static class UserStoreListener implements ExternalStoreListener<UserKey, UserValue> {

    private final AuthCache authCache;
    private final KafkaAuthWriter writer;
    private volatile int generationId;

    UserStoreListener(AuthCache authCache, KafkaAuthWriter writer) {
      this.authCache = authCache;
      this.writer = writer;
    }

    @Override
    public void initialize(Map<UserKey, UserValue> initialValues) {
      Map<KafkaPrincipal, UserMetadata> cachedUsers = authCache.users();
      cachedUsers.forEach((user, metadata) -> {
        UserKey key = new UserKey(user);
        UserValue value = initialValues.get(key);
        if (value != null) {
          if (!value.groups().equals(metadata.groups()))
            update(key, value);
        } else
          delete(key);
      });
      initialValues.entrySet().stream()
          .filter(e -> !cachedUsers.containsKey(e.getKey().principal()))
          .forEach(e -> update(e.getKey(), e.getValue()));
    }

    @Override
    public void update(UserKey key, UserValue value) {
      writer.writeExternalEntry(key, value, generationId);
    }

    @Override
    public void delete(UserKey key) {
      writer.writeExternalEntry(key, null, generationId);
    }

    @Override
    public void fail(String errorMessage) {
      writer.writeExternalStatus(MetadataStoreStatus.FAILED, errorMessage, generationId);
    }

    @Override
    public void resetFailure() {
      writer.writeExternalStatus(MetadataStoreStatus.INITIALIZED, null, generationId);
    }
  }
}
