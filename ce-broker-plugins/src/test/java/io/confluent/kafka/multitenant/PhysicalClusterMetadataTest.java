// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.google.common.collect.ImmutableSet;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static io.confluent.kafka.multitenant.quota.TenantQuotaCallback.DEFAULT_MIN_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.multitenant.quota.QuotaConfig;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;

public class PhysicalClusterMetadataTest {

  private static final Long RETRY_INITIAL_BACKOFF_MS = TimeUnit.SECONDS.toMillis(2);
  private static final Long RETRY_MAX_BACKOFF_MS = TimeUnit.SECONDS.toMillis(8);
  private static final LogicalClusterMetadata LC_META_XYZ =
      new LogicalClusterMetadata("lkc-xyz", "pkc-xyz", "xyz", "my-account", "k8s-abc",
                                 104857600L, 1024L, 2048L,
                                 LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
                                 LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);
  private static final LogicalClusterMetadata LC_META_ABC =
      new LogicalClusterMetadata("lkc-abc", "pkc-abc", "abc", "my-account", "k8s-abc",
                                 10485760L, 102400L, 204800L,
                                 LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
                                 LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);

  private PhysicalClusterMetadata lcCache;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    lcCache = new PhysicalClusterMetadata(RETRY_INITIAL_BACKOFF_MS, RETRY_MAX_BACKOFF_MS);
    lcCache.configure(tempFolder.getRoot().getCanonicalPath());
    // but not started, so we can test different initial state of the directory
  }

  @After
  public void tearDown() throws Exception {
    lcCache.shutdown();
  }

  @Test
  public void testCreateAndRemoveInstance() throws Exception {
    final String brokerUUID = "test-uuid";
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", String.valueOf(brokerUUID));
    // will create directory if it does not exist
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath() + "/subdir/anotherdir/");

    // get instance does not create instance
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));

    final PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
    assertTrue("Expected cache to be initialized", metadata.isUpToDate());
    assertEquals(metadata, PhysicalClusterMetadata.getInstance(brokerUUID));
    metadata.close(brokerUUID);
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));
  }

  @Test(expected = ConfigException.class)
  public void testConfigureInstanceWithoutDirConfigThrowsException() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", "test-uuid-1");
    final PhysicalClusterMetadata metadata = new PhysicalClusterMetadata();
    metadata.configure(configs);
  }

  @Test
  public void testConfigureInstanceWithSameBrokerUuid() throws IOException {
    final String brokerUUID = "test-uuid-2";
    Map<String, Object> configs = new HashMap<>();
    configs.put("broker.session.uuid", brokerUUID);
    configs.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());

    final PhysicalClusterMetadata meta1 = new PhysicalClusterMetadata();
    meta1.configure(configs);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    // configure() on the same instance and broker UUId does nothing
    meta1.configure(configs);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));

    final PhysicalClusterMetadata meta2 = new PhysicalClusterMetadata();
    // configuring another instance with the same broker uuid should fail
    try {
      meta2.configure(configs);
      fail("Exception not thrown when configuring another instance with the same broker UUID");
    } catch (UnsupportedOperationException e) {
      // success, but verify that we can still get an original instance for this broker UUID
      assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    }

    // close() on second instance which we failed to configure should not shutdown and remove the
    // original instance with the same broker UUID
    meta2.close(brokerUUID);
    assertEquals(meta1, PhysicalClusterMetadata.getInstance(brokerUUID));
    meta1.close(brokerUUID);
    assertNull(PhysicalClusterMetadata.getInstance(brokerUUID));
  }

  @Test
  public void testStartWithInaccessibleDirShouldThrowException() throws IOException {
    assertTrue(tempFolder.getRoot().setReadable(false));
    try {
      lcCache.start();
      fail("IOException not thrown when starting with inaccessible directory.");
    } catch (IOException ioe) {
      // success
      assertFalse(lcCache.isUpToDate());
      assertFalse(lcCache.isStale());
    }

    // should be able to start() once directory is readable
    assertTrue(tempFolder.getRoot().setReadable(true));
    lcCache.start();
    assertTrue(lcCache.isUpToDate());
  }

  @Test
  public void testExistingFilesLoaded() throws IOException, InterruptedException {
    createLogicalClusterFile(LC_META_ABC);

    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUpToDate());
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId()), lcCache.logicalClusterIds());
  }

  @Test
  public void testHandlesFileEvents() throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUpToDate());
    assertEquals(ImmutableSet.of(), lcCache.logicalClusterIds());

    // create new file and ensure cache gets updated
    final String lcId = LC_META_XYZ.logicalClusterId();
    Path lcFile = createLogicalClusterFile(LC_META_XYZ);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcId) != null,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(LC_META_XYZ, lcCache.metadata(LC_META_XYZ.logicalClusterId()));

    // update logical cluster
    LogicalClusterMetadata updatedLcMeta = new LogicalClusterMetadata(
        LC_META_XYZ.logicalClusterId(), LC_META_XYZ.physicalClusterId(),
        "new-name", "new-account", LC_META_XYZ.k8sClusterId(), LC_META_XYZ.storageBytes(),
        LC_META_XYZ.producerByteRate(), LC_META_XYZ.consumerByteRate(),
        LC_META_XYZ.requestPercentage(), LC_META_XYZ.networkQuotaOverhead()
    );
    Files.write(lcFile, jsonString(updatedLcMeta, true).getBytes());
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcId).logicalClusterName().equals("new-name"),
        "Expected metadata to be updated");

    // delete logical cluster
    Files.delete(lcFile);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcId) == null,
        "Expected metadata to be removed from the cache");
  }

  @Test
  public void testEventsForJsonFileWithInvalidContentDoNotImpactValidLogicalClusters()
      throws IOException, InterruptedException {
    Path fileWithInvalidContent = createLogicalClusterFile(LC_META_ABC, false);
    createLogicalClusterFile(LC_META_XYZ);

    lcCache.start();
    assertTrue("Expected invalid metadata to cause stale cache.", lcCache.isStale());
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId()), lcCache.logicalClusterIds());

    // update file with invalid content with another invalid content
    Files.write(fileWithInvalidContent, jsonString(LC_META_XYZ, false).getBytes());

    // we cannot verify that an update event was handled for already invalid file, so create
    // another valid file which should be an event after a file update event
    final LogicalClusterMetadata anotherMeta = new LogicalClusterMetadata(
        "lkc-123", "pkc-123", "123", "my-account", "k8s-123",
        10485760L, 102400L, 204800L, LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
        LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);
    createLogicalClusterFile(anotherMeta);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(anotherMeta.logicalClusterId()) != null,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), anotherMeta.logicalClusterId()),
                 lcCache.logicalClusterIds());

    // deleting file with invalid content should remove "stale" state
    Files.delete(fileWithInvalidContent);
    TestUtils.waitForCondition(
        lcCache::isUpToDate, "Deleting file with bad content should remove cache staleness.");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), anotherMeta.logicalClusterId()),
                 lcCache.logicalClusterIds());

    // ensure we can re-create file with the same name but with good content
    createLogicalClusterFile(LC_META_ABC);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        "Expected metadata of new logical cluster to be present in metadata cache");
    assertEquals(
        ImmutableSet.of(LC_META_XYZ.logicalClusterId(),
                        anotherMeta.logicalClusterId(),
                        LC_META_ABC.logicalClusterId()),
        lcCache.logicalClusterIds());
  }

  @Test
  public void testWatcherIsClosedAfterShutdown() throws IOException, InterruptedException {
    assertFalse(lcCache.dirWatcher.isRegistered());
    lcCache.start();
    // wait until WatchService is started, which happens on a separate thread
    TestUtils.waitForCondition(
        lcCache.dirWatcher::isRegistered, "Timed out waiting for WatchService to start.");
    lcCache.shutdown();
    assertFalse(lcCache.dirWatcher.isRegistered());
  }

  @Test(expected = IllegalStateException.class)
  public void testStartAfterShutdownShouldThrowException() throws IOException {
    lcCache.shutdown();
    lcCache.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testLogicalClusterIdsAfterShutdownShouldThrowException() throws IOException {
    lcCache.start();
    lcCache.shutdown();
    lcCache.logicalClusterIds();
  }

  @Test(expected = IllegalStateException.class)
  public void testLogicalClusterIdsBeforeStartShouldThrowException() {
    lcCache.logicalClusterIds();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetMetadataAfterShutdownShouldThrowException() throws IOException {
    lcCache.start();
    lcCache.shutdown();
    lcCache.metadata("some-cluster-id");
  }

  @Test(expected = IllegalStateException.class)
  public void testGetMetadataBeforeStartShouldThrowException() {
    assertFalse(lcCache.isStale());
    assertFalse(lcCache.isUpToDate());
    lcCache.metadata("some-cluster-id");
  }

  @Test
  public void testShouldSilentlySkipSubdirectoryEvents() throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUpToDate());

    final File subdir = tempFolder.newFolder("lkc-hjf");
    assertTrue(subdir.exists() && subdir.isDirectory());

    // we need to wait until the cache handles new subdir event, but there is no way to check
    // that it happened. Will create a new file as well, and hopefully that event will be behind
    // the `new dir` event
    createLogicalClusterFile(LC_META_ABC);
    createLogicalClusterFile(LC_META_XYZ);
    TestUtils.waitForCondition(
        () -> lcCache.logicalClusterIds().size() >= 2,
        "Expected two new logical clusters to be added to the cache.");

    assertEquals(LC_META_XYZ, lcCache.metadata(LC_META_XYZ.logicalClusterId()));
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertTrue(lcCache.isUpToDate());
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIds());
  }

  @Test
  public void testShouldRetryOnFailureToReadFile() throws IOException, InterruptedException {
    // create one file in logical clusters dir, but not readable
    Path abcMetaFile = createLogicalClusterFile(LC_META_ABC);
    Files.setPosixFilePermissions(abcMetaFile, PosixFilePermissions.fromString("-wx-wx-wx"));

    // we should be able to start the cache, but with scheduled retry
    lcCache.start();
    assertTrue(lcCache.isStale());

    // should be still able to update cache from valid files
    createLogicalClusterFile(LC_META_XYZ);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        "Expected metadata of 'lkc-xyz' logical cluster to be present in metadata cache");
    assertTrue(lcCache.isStale());

    // "fix" abc metadata file
    Files.setPosixFilePermissions(abcMetaFile, PosixFilePermissions.fromString("rwxrwxrwx"));

    TestUtils.waitForCondition(lcCache::isUpToDate, "Expected cache to recover");
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
  }

  @Test
  public void testShouldSkipInvalidJsonButUpdateCacheWhenJsonGetsFixed()
      throws IOException, InterruptedException {
    lcCache.start();
    assertTrue(lcCache.isUpToDate());

    // initially create json file with invalid content
    Path abcMetaFile = createLogicalClusterFile(LC_META_ABC, false);
    // and create one valid file so that we know when update event gets handled
    createLogicalClusterFile(LC_META_XYZ);

    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_XYZ.logicalClusterId()) != null,
        "Expected metadata of 'lkc-xyz' logical cluster to be present in metadata cache");
    TestUtils.waitForCondition(lcCache::isStale,
                               "Expected inaccessible logical cluster file to cause stale cache.");
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId()), lcCache.logicalClusterIds());
    assertEquals(ImmutableSet.of(LC_META_XYZ.logicalClusterId(), LC_META_ABC.logicalClusterId()),
                 lcCache.logicalClusterIdsIncludingStale());

    // "fix" abc cluster meta, which should cause cache update
    Files.write(abcMetaFile, jsonString(LC_META_ABC, true).getBytes());
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        "Expected metadata to be updated");
    assertTrue("Expect not stale cache anymore", lcCache.isUpToDate());
    assertEquals(ImmutableSet.of(LC_META_ABC.logicalClusterId(), LC_META_XYZ.logicalClusterId()),
                 lcCache.logicalClusterIds());
  }

  @Test
  public void testShouldSkipInvalidMetadataButUpdateCacheWhenFixed()
      throws IOException, InterruptedException {
    final LogicalClusterMetadata lcMeta =
        new LogicalClusterMetadata("lkc-qwr", "pkc-qwr", "xyz", "my-account", "k8s-abc",
                                   104857600L, 1024L, null,
                                   LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
                                   LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);

    lcCache.start();
    assertTrue(lcCache.isUpToDate());

    // initially create json file with valid json content, but invalid metadata
    Path metaFile = createLogicalClusterFile(lcMeta, true);
    TestUtils.waitForCondition(lcCache::isStale, "Expected invalid metadata to cause stale cache.");

    // "fix" cluster meta, which should cause cache update
    final LogicalClusterMetadata lcValidMeta = new LogicalClusterMetadata(
        "lkc-qwr", "pkc-qwr", "xyz", "my-account", "k8s-abc", 104857600L, 1024L, 2048L,
        LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE,
        LogicalClusterMetadata.DEFAULT_NETWORK_QUOTA_OVERHEAD_PERCENTAGE);
    Files.write(metaFile, jsonString(lcValidMeta, true).getBytes());
    TestUtils.waitForCondition(
        () -> lcCache.metadata(lcValidMeta.logicalClusterId()) != null,
        "Expected metadata to be updated");
    assertTrue("Expect not stale cache anymore", lcCache.isUpToDate());
    assertEquals(ImmutableSet.of(lcValidMeta.logicalClusterId()), lcCache.logicalClusterIds());
  }

  @Test
  public void testLoadAndRemoveMetadataCallsQuotaCallback()
      throws IOException, InterruptedException {
    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", "1"));
    Path metaFile = createLogicalClusterFile(LC_META_ABC);

    lcCache.start();
    assertTrue("Expected cache to be initialized", lcCache.isUpToDate());
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));

    Map<String, String> tags = Collections.singletonMap("tenant", LC_META_ABC.logicalClusterId());
    assertEquals(2.0 * 102400.0 / DEFAULT_MIN_PARTITIONS,
                 quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags), 0.001);
    assertEquals(2.0 * 204800.0 / DEFAULT_MIN_PARTITIONS,
                 quotaCallback.quotaLimit(ClientQuotaType.FETCH, tags), 0.001);
    assertEquals(LogicalClusterMetadata.DEFAULT_REQUEST_PERCENTAGE / DEFAULT_MIN_PARTITIONS,
                 quotaCallback.quotaLimit(ClientQuotaType.REQUEST, tags), 0.001);

    Files.delete(metaFile);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) == null,
        "Expected metadata to be removed from the cache");
    TestUtils.waitForCondition(
        () -> quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, tags) ==
              QuotaConfig.UNLIMITED_QUOTA.quota(ClientQuotaType.PRODUCE),
        "Expected unlimited quota for tenant with no quota");
  }

  @Test
  public void testShouldSilentlySkipApiKeysAndHealthcheckFiles()
      throws IOException, InterruptedException {
    final String apikeysJson = "{\"keys\": {\"key1\": {" +
                            "\"user_id\": \"user1\"," +
                            "\"logical_cluster_id\": \"myCluster\"," +
                            "\"sasl_mechanism\": \"PLAIN\"," +
                            "\"hashed_secret\": \"no hash\"," +
                            "\"hash_function\": \"none\"" +
                            "}}}";
    final Path apikeysFile = tempFolder.newFile("apikeys.json").toPath();
    Files.write(apikeysFile, apikeysJson.getBytes());

    final String hcJson = "{\"kafka_key\":\"Q4L43O\",\"kafka_secret\":\"J\",\"dd_api_key\":\"\"}";
    final Path hcFile = tempFolder.newFile("kafka-healthcheck-external.json").toPath();
    Files.write(hcFile, hcJson.getBytes());

    lcCache.start();
    assertTrue(lcCache.isUpToDate());

    Path lcFile = createLogicalClusterFile(LC_META_ABC);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) != null,
        "Expected new logical cluster to be added to the cache.");
    assertEquals(LC_META_ABC, lcCache.metadata(LC_META_ABC.logicalClusterId()));
    assertTrue(lcCache.isUpToDate());

    // writing the same content will still trigger file update event
    Files.write(apikeysFile, apikeysJson.getBytes());

    // since the file update happens async, remove the valid metadata file and hopefully that
    // update will happen after the previous update
    Files.delete(lcFile);
    TestUtils.waitForCondition(
        () -> lcCache.metadata(LC_META_ABC.logicalClusterId()) == null,
        "Expected metadata to be removed from the cache");

    assertTrue(lcCache.isUpToDate());
  }

  private Path createLogicalClusterFile(LogicalClusterMetadata lcMeta) throws IOException {
    return createLogicalClusterFile(lcMeta, true);
  }

  private Path createLogicalClusterFile(LogicalClusterMetadata lcMeta, boolean valid)
      throws IOException {
    final String lcFilename = lcMeta.logicalClusterId() + ".json";
    final Path lcFile = tempFolder.newFile(lcFilename).toPath();
    Files.write(lcFile, jsonString(lcMeta, valid).getBytes());
    return lcFile;
  }

  private String jsonString(LogicalClusterMetadata lcMeta, boolean valid) {
    String json = "{" +
                        "\"logical_cluster_id\": \"" + lcMeta.logicalClusterId() + "\"," +
                        "\"physical_cluster_id\": \"" + lcMeta.physicalClusterId() + "\"," +
                        "\"logical_cluster_name\": \"" + lcMeta.logicalClusterName() + "\"," +
                        "\"account_id\": \"" + lcMeta.accountId() + "\"," +
                        "\"k8s_cluster_id\": \"" + lcMeta.k8sClusterId() + "\"," +
                        "\"logical_cluster_type\": \"" + lcMeta.logicalClusterType() + "\"";
    if (lcMeta.storageBytes() != null) {
      json += ", \"storage_bytes\": " + lcMeta.storageBytes();
    }
    if (lcMeta.producerByteRate() != null) {
      json += ", \"network_ingress_byte_rate\": " + lcMeta.producerByteRate();
    }
    if (lcMeta.consumerByteRate() != null) {
      json += ", \"network_egress_byte_rate\": " + lcMeta.consumerByteRate();
    }
    if (valid) {
      json += "}";
    }
    return json;
  }
}
