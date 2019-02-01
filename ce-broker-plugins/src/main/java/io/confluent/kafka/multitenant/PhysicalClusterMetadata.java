// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant;


import com.google.common.collect.Sets;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.server.multitenant.MultiTenantMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.StandardWatchEventKinds;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.multitenant.quota.QuotaConfig;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import io.confluent.kafka.utils.RetryBackoff;
import kafka.server.KafkaConfig$;

/**
 * This holds metadata passed from CCloud related to this physical cluster
 */
public class PhysicalClusterMetadata implements MultiTenantMetadata {

  private static final Map<String, PhysicalClusterMetadata> INSTANCES = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalClusterMetadata.class);

  private static final String LOGICAL_CLUSTER_FILE_EXT_WITH_DOT = ".json";
  private static final Long CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  private static final Long RETRY_INITIAL_BACKOFF_MS = TimeUnit.MINUTES.toMillis(5);
  private static final Long RETRY_MAX_BACKOFF_MS = TimeUnit.MINUTES.toMillis(30);

  private String logicalClustersDir;
  // logical cluster ID --> LogicalClusterMetadata
  private final Map<String, LogicalClusterMetadata> logicalClusterMap;

  final LogicalClustersChangeListener dirWatcher;
  private final Thread dirListenerThread;
  private final ScheduledExecutorService executorService;
  private final RetryBackoff retryBackoff;
  private volatile Future<?> reloadFuture = null;
  private int retryCount;
  private final ReadWriteLock cacheLock;

  public enum State {
    NOT_READY,
    UP_TO_DATE,
    STALE,
    CLOSED
  }
  private final AtomicReference<State> state;
  // either newly created logical clusters that we failed to load (not in logicalClusterMap) or
  // logical clusters that were updated but we failed to load the updated file
  private final Set<String> staleLogicalClusters;


  public PhysicalClusterMetadata() {
    this(RETRY_INITIAL_BACKOFF_MS, RETRY_MAX_BACKOFF_MS);
  }

  // used by unit test
  PhysicalClusterMetadata(Long initialBackoffMs, Long maxBackoffMs) {
    this.state = new AtomicReference<>(State.NOT_READY);
    this.cacheLock = new ReentrantReadWriteLock();
    this.logicalClusterMap = new ConcurrentHashMap<>();
    this.staleLogicalClusters = new CopyOnWriteArraySet<>();
    this.dirWatcher = new LogicalClustersChangeListener();
    this.dirListenerThread = new Thread(this.dirWatcher, "confluent-tenants-change-listener");
    this.executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      final Thread thread = new Thread(runnable, "physical-cluster-metadata-retry");
      thread.setDaemon(true);
      return thread;
    });
    this.retryCount = 0;
    this.retryBackoff = new RetryBackoff(initialBackoffMs.intValue(), maxBackoffMs.intValue());
  }

  /**
   * Loads the cache and starts listening for directory events in directory specified in
   * ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG config. Adds the instance for the given
   * broker session UUID, specified in KafkaConfig.BrokerSessionUuidProp, to the static
   * instance map. The caller of this method must call close() when done to remove the instance
   * from the static map.
   * @param configs broker configuration
   * @throws ConfigException if KafkaConfig.BrokerSessionUuidProp is not set, or
   *         ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG is not set.
   * @throws UnsupportedOperationException if another instance of this class with the same broker
   *         session UUID was already configured.
   */
  @Override
  public void configure(Map<String, ?> configs) {
    String instanceKey = getInstanceKey(configs);
    Object dirConfigValue = configs.get(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG);
    if (dirConfigValue == null) {
      throw new ConfigException(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG + " is not set");
    }
    this.logicalClustersDir = dirConfigValue.toString();
    synchronized (INSTANCES) {
      PhysicalClusterMetadata instance = INSTANCES.get(instanceKey);
      if (instance == null) {
        INSTANCES.put(instanceKey, this);
      } else if (this != instance) {
        // we don't want to silently start a different instance with the same broker session UUID
        // to avoid strange/silent bugs, since getting an instance will always return the first
        // instance configured for this broker session UUID.
        throw new UnsupportedOperationException(
            "PhysicalClusterMetadata instance already exists for broker session " + instanceKey);
      } else {
        LOG.info("Skipping configuring this instance (broker session {}): Already configured.",
                 instanceKey);
        return;
      }
    }

    try {
      start();
    } catch (IOException ioe) {
      close(instanceKey);
      throw new ConfigException("Failed to load PhysicalClusterMetadata: " + ioe.getMessage());
    }
    LOG.warn("Configured and started instance for broker session {}", instanceKey);
  }

  // used by unit test
  void configure(String logicalClustersDir) {
    this.logicalClustersDir = logicalClustersDir;
  }

  @Override
  public void close(String brokerSessionUuid) {
    synchronized (INSTANCES) {
      PhysicalClusterMetadata instance = INSTANCES.get(brokerSessionUuid);
      if (instance != null && instance == this) {
        INSTANCES.remove(brokerSessionUuid);
        LOG.info("Removed instance for broker session {}", brokerSessionUuid);
      } else if (instance != null) {
        LOG.info("Closing instance that doesn't match the instance in the static map with the same"
                 + " broker session {} will not remove that instance from the map.", brokerSessionUuid);
      }
    }
    shutdown();
  }

  public static PhysicalClusterMetadata getInstance(String brokerSessionUuid) {
    synchronized (INSTANCES) {
      return INSTANCES.get(brokerSessionUuid);
    }
  }

  /**
   * Loads the cache and starts listening for directory events
   * @throws IllegalStateException if PhysicalClusterMetadata was already shut down
   * @throws IOException if failed to register watcher for metadata updates, which likely means
   *         that the logical clusters directory is inaccessible.
   */
  void start() throws IOException {
    if (State.CLOSED.equals(state.get())) {
      throw new IllegalStateException("Physical Cluster Metadata Cache already shut down.");
    }

    if (state.compareAndSet(State.NOT_READY, State.STALE)) {
      try {
        dirWatcher.register(logicalClustersDir);
      } catch (IOException ioe) {
        state.compareAndSet(State.STALE, State.NOT_READY);
        LOG.error("Failed to register watcher for dir={}", logicalClustersDir, ioe);
        throw ioe;
      }
      if (!loadAllFiles()) {
        scheduleReloadCache();
      }
      maybeSetNotStale();
      LOG.info("Loaded logical cluster metadata from files in dir={} state={}",
               logicalClustersDir, state.get());
      dirListenerThread.start();
    }
  }

  /**
   * After this method is called, querying the cache will throw IllegalStateException
   */
  void shutdown() {
    LOG.info("Shutting down");
    if (state.getAndSet(State.CLOSED) != State.CLOSED) {
      try {
        dirListenerThread.interrupt();
        dirListenerThread.join(CLOSE_TIMEOUT_MS);
      } catch (InterruptedException ie) {
        LOG.error("Shutting down tenant metadata listener thread was interrupted", ie);
      }

      if (reloadFuture != null) {
        reloadFuture.cancel(true);
      }
      executorService.shutdownNow();
      try {
        executorService.awaitTermination(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.debug("Shutting down was interrupted", e);
      }
      LOG.info("Closed Physical Cluster Metadata Cache");
    }
  }

  /**
   * Returns true if cache is loaded and up-to-date. Returns false if cache is stale, or not open.
   */
  public boolean isUpToDate() {
    return State.UP_TO_DATE.equals(state.get());
  }

  /**
   * Returns true if there was an issue with handling add/update/delete of at least one logical
   * cluster, and was not able to recover yet.
   */
  public boolean isStale() {
    return State.STALE.equals(state.get());
  }

  /**
   * Returns all logical clusters with up-to-date/valid metadata hosted by this physical cluster
   * @return set of logical cluster IDs
   * @throws IllegalStateException if cache is not started or already shut down
   */
  public Set<String> logicalClusterIds() {
    ensureOpen();
    return Sets.difference(logicalClusterMap.keySet(), staleLogicalClusters).immutableCopy();
  }

  /**
   * Returns all logical clusters hosted by this physical cluster, including logical clusters
   * with stale/invalid metadata
   * @return set of logical cluster IDs
   * @throws IllegalStateException if cache is not started or already shut down
   */
  public Set<String> logicalClusterIdsIncludingStale() {
    ensureOpen();
    return Sets.union(logicalClusterMap.keySet(), staleLogicalClusters).immutableCopy();
  }

  /**
   * Returns metadata of a given logical cluster ID
   * @param logicalClusterId logical cluster ID
   * @return logical cluster metadata or null if logical cluster does not exist or its metadata
   *         is stale
   * @throws IllegalStateException if cache is not started yet or already shut down
   */
  public LogicalClusterMetadata metadata(String logicalClusterId) {
    ensureOpen();
    if (!staleLogicalClusters.contains(logicalClusterId)) {
      return logicalClusterMap.get(logicalClusterId);
    }
    return null;
  }

  /* Private methods below */

  private static String getInstanceKey(Map<String, ?> configs) {
    Object uuidConfigValue = configs.get(KafkaConfig$.MODULE$.BrokerSessionUuidProp());
    if (uuidConfigValue == null) {
      throw new ConfigException(KafkaConfig$.MODULE$.BrokerSessionUuidProp() + " is not set");
    }
    return uuidConfigValue.toString();
  }

  private void ensureOpen() {
    State curState = state.get();
    if (State.NOT_READY.equals(curState)) {
      throw new IllegalStateException("Physical Cluster Metadata Cache not started.");
    }
    if (State.CLOSED.equals(curState)) {
      throw new IllegalStateException("Physical Cluster Metadata Cache already shutdown");
    }
  }

  /**
   * Loads every file from the logical clusters directory and updates the cache
   * @return true if successfully loaded the files (excluding files with bad content or
   *         extension); otherwise false which means need to retry
   */
  private boolean loadAllFiles() {
    try (Stream<Path> fileStream = Files.list(Paths.get(logicalClustersDir))) {
      long numFilesNeedRetry = fileStream
              .map(this::loadLogicalClusterMetadata)
              .filter(retry -> retry)
              .count();
      return numFilesNeedRetry == 0;
    } catch (IOException ioe) {
      LOG.warn("Failed to read metadata files from dir={}", logicalClustersDir);
    }
    return false;
  }

  /**
   * Loads all the files in the logical clusters directory and updates cache if any of the files
   * has a different content. This is done under the big lock, and dir watcher updates will be
   * queued until after the cache is re-loaded. It is possible, that file create/update event
   * will be processed after the file was already loaded by this method, but it does not break
   * correctness of the cache, since the most recent content of the file will be re-loaded.
   */
  private void reloadCache() {
    // this method is called in a separate thread and the only thread that will be waiting on
    // this lock is directory watcher thread (not main thread);
    cacheLock.writeLock().lock();
    try {
      if (!State.CLOSED.equals(state.get())) {
        LOG.info(
            "Re-loading cache (attempt={}, current state={}, (known) stale logical clusters={})",
            retryCount, state.get(), staleLogicalClusters);
        staleLogicalClusters.clear();
        if (!loadAllFiles()) {
          LOG.warn("Failed to recover cache after {} attempts. Will retry again.", retryCount);
          if (reloadFuture != null) {
            reloadFuture.cancel(false);
          }
          scheduleReloadCache();
        } else {
          maybeSetNotStale();
        }
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  /**
   * Schedules retry to load the cache, if it has not been scheduled yet.
   */
  private void scheduleReloadCache() {
    if (reloadFuture == null || reloadFuture.isDone()) {
      int backoffMs = retryBackoff.backoffMs(retryCount++);
      LOG.info("Scheduling re-loading logical clusters cache in {} ms", backoffMs);
      reloadFuture = executorService.schedule(this::reloadCache, backoffMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Mark given logical cluster ID as stale, which also makes cache state stale
   * @param logicalClusterId ID of the logical cluster that failed re-fresh
   */
  private void markStale(String logicalClusterId) {
    if (state.compareAndSet(State.UP_TO_DATE, State.STALE) ||
        state.compareAndSet(State.NOT_READY, State.STALE) ||
        state.compareAndSet(State.STALE, State.STALE)) {
      staleLogicalClusters.add(logicalClusterId);
    } else {
      throw new IllegalStateException("Unexpected cache state: " + state.get());
    }
  }

  private void markUpToDate(String logicalClusterId) {
    if (staleLogicalClusters.remove(logicalClusterId)) {
      maybeSetNotStale();
    }
  }

  private void maybeSetNotStale() {
    if (staleLogicalClusters.isEmpty() &&
        state.compareAndSet(State.STALE, State.UP_TO_DATE)) {
      LOG.info("Cache is up to date");
      retryCount = 0;
    }
  }

  /**
   * Called by directory watcher thread when given file is created or updated. Updates metadata
   * of the corresponding logical cluster.
   * @param lcFile file to load
   */
  private void updateLogicalClusterMetadata(Path lcFile) {
    // big cache lock to make sure that this update does not happen in the middle of the whole
    // cache re-load
    cacheLock.readLock().lock();
    try {
      if (loadLogicalClusterMetadata(lcFile)) {
        scheduleReloadCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  /**
   * Updates logical cluster metadata from given file
   * @param lcFile file to load
   * @return true if need to re-load the cache; otherwise false
   */
  private boolean loadLogicalClusterMetadata(Path lcFile) {
    String logicalClusterId = logicalClusterId(lcFile);
    if (logicalClusterId == null || !Files.isRegularFile(lcFile)) {
      // ignore directories or files with a non-json extension
      LOG.warn("Ignoring create/update of a non-json file {}", lcFile);
      return false;
    }

    try {
      ObjectMapper objectMapper = new ObjectMapper();
      LogicalClusterMetadata lcMeta = objectMapper.readValue(
          lcFile.toFile(), LogicalClusterMetadata.class);
      if (lcMeta.logicalClusterId() == null) {
        // currently, few other .json files get synced to the same dir (which we are going to
        // consolidate into tenant metadata files later): apikeys.json and healthcheck apikeys
        // With @JsonIgnoreProperties(ignoreUnknown = true), we will be able to load
        // apikeys and healthcheck files, but they will not have "logical_cluster_id" field
        LOG.info("Ignoring create/update of {}", lcFile.getFileName());
        return false;
      } else if (!logicalClusterId.equals(lcMeta.logicalClusterId()) || !lcMeta.isValid()) {
        LOG.warn("Logical cluster file {} has invalid metadata {}.", lcFile, lcMeta);
        markStale(logicalClusterId);
        return false;
      }
      logicalClusterMap.put(lcMeta.logicalClusterId(), lcMeta);
      markUpToDate(logicalClusterId);
      LOG.info("Added/Updated logical cluster {}", lcMeta);

      // for now updating all the quotas
      updateQuotas();
    } catch (JsonParseException jse) {
      LOG.error("Error parsing metadata file for logical cluster {}", logicalClusterId, jse);
      // not going to retry, because fixing the content will cause an UPDATE event
      markStale(logicalClusterId);
    } catch (Exception e) {
      // ObjectMapper behavior of loading json files that do not exactly match the format of
      // LogicalClusterMetadata is inconsistent, even in the same environment. So, we need to filter
      // apikeys and healthcheck file with apikeys here as well, but by filename
      String filename = lcFile.getFileName().toString();
      if ((filename.contains("apikeys") || filename.contains("healthcheck")) &&
          !filename.contains("lkc")) {
        LOG.info("Ignoring create/update of {}", lcFile.getFileName());
        return false;
      }

      LOG.error("Failed to load metadata file for logical cluster {}", logicalClusterId, e);
      markStale(logicalClusterId);
      return true;
    }
    return false;
  }

  private static QuotaConfig quotaConfig(LogicalClusterMetadata lcMeta) {
    double multiplier = 1 + lcMeta.networkQuotaOverhead() / 100.0;
    return new QuotaConfig((long) (multiplier * lcMeta.producerByteRate()),
                           (long) (multiplier * lcMeta.consumerByteRate()),
                           lcMeta.requestPercentage(),
                           QuotaConfig.UNLIMITED_QUOTA);
  }

  private void updateQuotas() {
    Map<String, QuotaConfig> tenantQuotas = logicalClusterMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> quotaConfig(e.getValue())));
    TenantQuotaCallback.updateQuotas(tenantQuotas, QuotaConfig.UNLIMITED_QUOTA);
  }

  private void removeLogicalClusterMetadata(Path lcFile) {
    String logicalClusterId = logicalClusterId(lcFile);
    if (logicalClusterId == null) {
      LOG.warn("Ignoring deletion of a non-json file {}", lcFile);
      return;
    }

    cacheLock.readLock().lock();
    try {
      LogicalClusterMetadata deletedMeta = logicalClusterMap.remove(logicalClusterId);
      markUpToDate(logicalClusterId);
      if (deletedMeta != null) {
        LOG.info("Removed logical cluster {}", logicalClusterId);
        updateQuotas();
      } else {
        LOG.warn("Got delete event for unknown or invalid logical cluster {}", logicalClusterId);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  /**
   * Returns logical cluster ID from the given file path, which is a name of the file denoted by
   * given path without logical cluster file extension. If the name of the file does not end with
   * .json extension, the method returns null.
   */
  private static String logicalClusterId(Path lcFile) {
    String fileName = lcFile.getFileName().toString();
    int indexOfDot = fileName.lastIndexOf(LOGICAL_CLUSTER_FILE_EXT_WITH_DOT);
    return indexOfDot < 0 ? null : fileName.substring(0, indexOfDot);
  }

  class LogicalClustersChangeListener implements Runnable {

    private WatchService watchService = null;
    private Path logicalClustersDirPath = null;

    public void register(String watchDir) throws IOException {
      watchService = FileSystems.getDefault().newWatchService();
      logicalClustersDirPath = Paths.get(watchDir);
      if (!Files.exists(logicalClustersDirPath)) {
        Files.createDirectories(logicalClustersDirPath);
      }
      logicalClustersDirPath.register(watchService,
                                      StandardWatchEventKinds.ENTRY_CREATE,
                                      StandardWatchEventKinds.ENTRY_MODIFY,
                                      StandardWatchEventKinds.ENTRY_DELETE);
    }

    public void close() {
      if (watchService != null) {
        try {
          watchService.close();
          // this is to be able to verify the watch service is closed in unit tests
          watchService = null;
          LOG.info("Closed watcher for {}", logicalClustersDir);
        } catch (IOException ioe) {
          LOG.error("Failed to shutdown watcher for {}.", logicalClustersDir, ioe);
        }
      }
    }

    // used in unit tests
    boolean isRegistered() {
      return watchService != null;
    }

    public void run() {
      LOG.info("Starting listening for changes in {}", logicalClustersDir);
      try {
        runWatcher(watchService, logicalClustersDirPath);
      } catch (InterruptedException ie) {
        LOG.warn("Watching {} was interrupted.", logicalClustersDir);
      } finally {
        close();
      }
    }

    private void runWatcher(WatchService watchService, Path watchDir) throws InterruptedException {
      boolean valid = true;
      while (valid) {
        WatchKey watchKey = watchService.take();
        for (WatchEvent<?> event: watchKey.pollEvents()) {
          LOG.trace("Got event: {} {}", event.kind(), event.context());
          @SuppressWarnings("unchecked")
          Path filename = watchDir.resolve(((WatchEvent<Path>) event).context());
          if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
            removeLogicalClusterMetadata(filename);
          } else {
            // create or update
            updateLogicalClusterMetadata(filename);
          }
        }
        valid = watchKey.reset();
      }
      LOG.warn("Watch key no longer registered for {}. Stopped watching.", watchDir);
    }

  }

}
