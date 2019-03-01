// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.provider.ldap.LdapStore;
import io.confluent.security.rbac.Scope;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.auth.provider.ldap.LdapAuthorizerConfig;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.external.ExternalStore;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.KafkaPartitionWriter;
import io.confluent.security.store.kafka.clients.CachedRecord;
import io.confluent.security.store.kafka.clients.Writer;
import io.confluent.security.store.kafka.coordinator.MetadataServiceRebalanceListener;
import io.confluent.security.auth.store.data.AuthEntryType;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleAssignmentKey;
import io.confluent.security.auth.store.data.RoleAssignmentValue;
import io.confluent.security.store.kafka.clients.ConsumerListener;
import io.confluent.security.rbac.AccessPolicy;
import io.confluent.security.rbac.InvalidRoleAssignmentException;
import io.confluent.security.rbac.Role;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.Context;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaAuthWriter implements Writer, AuthWriter, ConsumerListener<AuthKey, AuthValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAuthWriter.class);

  private final String topic;
  private final KafkaStoreConfig config;
  private final Time time;
  private final DefaultAuthCache authCache;
  private final Producer<AuthKey, AuthValue> producer;
  private final Map<AuthEntryType, ExternalStore<? extends AuthKey, ? extends AuthValue>> externalAuthStores;
  private final AtomicBoolean isMasterWriter;
  private Map<Integer, KafkaPartitionWriter<AuthKey, AuthValue>> partitionWriters;

  public KafkaAuthWriter(String topic,
                         KafkaStoreConfig config,
                         Producer<AuthKey, AuthValue> producer,
                         DefaultAuthCache authCache,
                         Time time) {
    this.topic = topic;
    this.config = config;
    this.producer = producer;
    this.authCache = authCache;
    this.time = time;
    this.externalAuthStores = new HashMap<>();
    this.isMasterWriter = new AtomicBoolean();
    loadExternalAuthStores();
  }

  public void start(int numPartitions, MetadataServiceRebalanceListener rebalanceListener) {
    if (numPartitions == 0)
      throw new IllegalStateException("Number of partitions not known for " + topic);
    this.partitionWriters = new HashMap<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      TopicPartition tp = new TopicPartition(topic, i);
      partitionWriters.put(i,
          new KafkaPartitionWriter<>(tp, producer, authCache, rebalanceListener, config.refreshTimeout, time));
    }

    log.debug("Created writer for topic {} with {} partitions", topic, partitionWriters.size());
  }

  @Override
  public void startWriter(int generationId) {
    if (generationId < 0)
      throw new IllegalArgumentException("Invalid generation id for master writer " + generationId);

    StatusValue initializing = new StatusValue(MetadataStoreStatus.INITIALIZING, generationId, null);
    partitionWriters.forEach((partition, writer) ->
        writer.start(generationId, new StatusKey(partition), initializing));

    externalAuthStores.forEach((type, store) -> store.start(generationId));

    StatusValue initialized = new StatusValue(MetadataStoreStatus.INITIALIZED, generationId, null);
    partitionWriters.forEach((partition, writer) ->
        writer.onInitializationComplete(generationId, new StatusKey(partition), initialized));
    isMasterWriter.set(true);
  }

  @Override
  public void stopWriter(Integer generationId) {
    isMasterWriter.set(false);
    externalAuthStores.values().forEach(store -> store.stop(generationId));
    if (partitionWriters != null)
      partitionWriters.values().forEach(p -> p.stop(generationId));
  }

  @Override
  public CompletionStage<Void> addRoleAssignment(KafkaPrincipal principal, String role, String scope) {
    log.debug("addRoleAssignment {} {} {}", principal, role, scope);
    return setRoleResources(principal, role, scope, Collections.emptySet());
  }

  @Override
  public CompletionStage<Void> addRoleResources(KafkaPrincipal principal,
                                       String role,
                                       String scope,
                                       Collection<Resource> newResources) {
    log.debug("addRoleResources {} {} {} {}", principal, role, scope, newResources);
    validateAssignmentUpdate(role, scope, newResources);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    CachedRecord<AuthKey, AuthValue> existingRecord =
        waitForExistingAssignment(partitionWriter, principal, role, scope);
    Set<Resource> updatedResources = resources(existingRecord);
    updatedResources.addAll(newResources);

    log.debug("New assignment {} {} {} {}", principal, role, scope, updatedResources);
    return partitionWriter.write(existingRecord.key(),
        new RoleAssignmentValue(updatedResources),
        existingRecord.generationIdDuringRead());
  }

  @Override
  public CompletionStage<Void> setRoleResources(KafkaPrincipal principal,
                                       String role,
                                       String scope,
                                       Collection<Resource> resources) {
    log.debug("setRoleResources {} {} {} {}", principal, role, scope, resources);
    validateAssignmentUpdate(role, scope, resources);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    RoleAssignmentKey key = new RoleAssignmentKey(principal, role, scope);

    return partitionWriter.write(key, new RoleAssignmentValue(resources), null);
  }

  @Override
  public CompletionStage<Void> removeRoleAssignment(KafkaPrincipal principal, String role, String scope) {
    log.debug("removeRoleAssignment {} {} {}", principal, role, scope);
    validateAssignmentUpdate(role, scope, Collections.emptySet());

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    RoleAssignmentKey key = new RoleAssignmentKey(principal, role, scope);

    return partitionWriter.write(key, null, null);
  }

  @Override
  public CompletionStage<Void> removeRoleResources(KafkaPrincipal principal,
                                          String role,
                                          String scope,
                                          Collection<Resource> deletedResources) {
    log.debug("removeRoleResources {} {} {} {}", principal, role, scope, deletedResources);
    validateAssignmentUpdate(role, scope, deletedResources);

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(principal, role, scope);
    CachedRecord<AuthKey, AuthValue> existingRecord =
        waitForExistingAssignment(partitionWriter, principal, role, scope);
    Set<Resource> updatedResources = resources(existingRecord);
    updatedResources.removeAll(deletedResources);
    RoleAssignmentValue value = new RoleAssignmentValue(updatedResources);

    log.debug("New assignment {} {} {} {}", principal, role, scope, updatedResources);
    return partitionWriter.write(
        existingRecord.key(),
        value,
        existingRecord.generationIdDuringRead());
  }

  public void close(Duration closeTimeout) {
    stopWriter(null);
    producer.close(closeTimeout);
  }

  @Override
  public void onConsumerRecord(ConsumerRecord<AuthKey, AuthValue> record, AuthValue oldValue) {
    // If writing is not enabled yet, we can ignore the record.
    if (partitionWriters == null)
      return;

    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriter(record.partition());
    AuthEntryType entryType = record.key().entryType();

    if (entryType == AuthEntryType.STATUS) {
      StatusValue statusValue = (StatusValue)  record.value();
      partitionWriter.onStatusConsumed(record.offset(), statusValue.generationId(), statusValue.status());
    } else {
      // If value hasn't changed, then it could be a duplicate whose write entry was
      // already cancelled and removed.
      boolean expectPendingWrite = !Objects.equals(record.value(), oldValue);
      partitionWriter.onRecordConsumed(record, oldValue, expectPendingWrite);
    }
  }

  public void write(AuthKey key, AuthValue value, Integer expectedGenerationId) {
    partitionWriter(partition(key)).write(key, value, expectedGenerationId);
  }

  private CachedRecord<AuthKey, AuthValue> waitForExistingAssignment(
      KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter,
      KafkaPrincipal principal,
      String role,
      String scope) {
    RoleAssignmentKey key = new RoleAssignmentKey(principal, role, scope);
    return partitionWriter.waitForRefresh(key);
  }

  private AccessPolicy accessPolicy(String role) {
    Role roleDefinition = authCache.rbacRoles().role(role);
    if (roleDefinition == null)
      throw new InvalidRoleAssignmentException("Role not found " + role);
    else
      return roleDefinition.accessPolicy();
  }

  private void validateAssignmentUpdate(String role, String scope, Collection<Resource> resources) {
    if (!isMasterWriter.get())
      throw new NotMasterWriterException("This node is currently not the master writer for Metadata Service."
        + " This could be a transient exception during writer election.");

    AccessPolicy accessPolicy = accessPolicy(role);
    if (!resources.isEmpty() && !accessPolicy.hasResourceScope())
      throw new IllegalArgumentException("Resources cannot be specified for role " + role +
          " with scope " + accessPolicy.scope());
    else if (resources.isEmpty() && accessPolicy.hasResourceScope())
      log.debug("Role assignment update of resource-scope role without any resources");

    if (!authCache.rootScope().containsScope(new Scope(scope))) {
      throw new InvalidScopeException("This writer does not contain assignment scope " + scope);
    }
  }

  private Set<Resource> resources(CachedRecord<AuthKey, AuthValue> record) {
    Set<Resource> resources = new HashSet<>();
    AuthValue value = record.value();
    if (value != null) {
      if (!(value instanceof RoleAssignmentValue))
        throw new IllegalArgumentException("Invalid record key=" + record.key() + ", value=" + value);
      resources.addAll(((RoleAssignmentValue) value).resources());
    }
    return resources;
  }

  private int partition(AuthKey key) {
    return Utils.toPositive(key.hashCode()) % partitionWriters.size();
  }

  private KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter(int partition) {
    KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter = partitionWriters.get(partition);
    if (partitionWriter == null)
      throw new IllegalArgumentException("Partition writer not found for partition " + partition);
    return partitionWriter;
  }

  private KafkaPartitionWriter<AuthKey, AuthValue> partitionWriter(KafkaPrincipal principal,
      String role,
      String scope) {
    RoleAssignmentKey key = new RoleAssignmentKey(principal, role, scope);
    return partitionWriter(partition(key));
  }

  private void loadExternalAuthStores() {
    Map<String, ?> configs = config.originals();
    if (configs.containsKey(LdapAuthorizerConfig.CONFIG_PREFIX + Context.PROVIDER_URL)) {
      LdapStore ldapStore = new LdapStore(authCache, this, time);
      ldapStore.configure(configs);
      externalAuthStores.put(AuthEntryType.USER, ldapStore);
    } else {
      externalAuthStores.put(AuthEntryType.USER, new DummyUserStore());
    }
  }

  private class DummyUserStore implements ExternalStore<UserKey, UserValue> {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void start(int generationId) {
      authCache.map(AuthEntryType.USER.name()).forEach((k, v) ->
          write(k, null, generationId));
    }

    @Override
    public void stop(Integer generationId) {
    }
  }
}