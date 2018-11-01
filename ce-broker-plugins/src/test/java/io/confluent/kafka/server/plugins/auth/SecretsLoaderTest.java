package io.confluent.kafka.server.plugins.auth;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.testing.FakeTicker;
import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import java.util.Collections;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.multitenant.quota.TenantQuotaCallback.DEFAULT_MIN_PARTITIONS;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SecretsLoaderTest {
  File tempFile;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    final String origJson = "{\"keys\": {\"key1\": {" +
        "\"user_id\": \"user1\"," +
        "\"logical_cluster_id\": \"myCluster\"," +
        "\"sasl_mechanism\": \"PLAIN\"," +
        "\"hashed_secret\": \"no hash\"," +
        "\"hash_function\": \"none\"" +
        "}}}";
    tempFile = tempFolder.newFile("apikeys.json");
    Files.write(origJson, tempFile, Charsets.UTF_8);
  }

  @Test
  public void testLoaderRefreshes() throws Exception {
    final String newJson = "{\"keys\": {\"key1\": {" +
        "\"user_id\": \"user1\"," +
        "\"logical_cluster_id\": \"myCluster\"," +
        "\"sasl_mechanism\": \"covfefe\"," +
        "\"hashed_secret\": \"no hash\"," +
        "\"hash_function\": \"none\"" +
        "}}}";
    final long refreshMs = 1000;

    FakeTicker fakeTicker = new FakeTicker();
    SecretsLoader loader = new SecretsLoader(
        tempFile.getCanonicalPath(),
        refreshMs,
        fakeTicker
    );

    Map<String, KeyConfigEntry> keys = loader.get();
    assertEquals("PLAIN", keys.get("key1").saslMechanism);

    Files.write(newJson, tempFile, Charsets.UTF_8);

    keys = loader.get();
    assertEquals("Original value remains until refresh", "PLAIN", keys.get("key1").saslMechanism);

    fakeTicker.advance(refreshMs + 1, TimeUnit.MILLISECONDS);
    keys = loader.get();
    assertEquals("New value is read", "covfefe", keys.get("key1").saslMechanism);
  }

  @Test
  public void shouldLoadFile() throws Exception {
    TenantConfig tenantConfig = SecretsLoader.loadFile(tempFile.getPath());
    Map<String, KeyConfigEntry> keys = tenantConfig.apiKeys;
    assertEquals(1, keys.size());
    KeyConfigEntry entry = keys.get("key1");
    assertNotNull(entry);
    assertEquals("PLAIN", entry.saslMechanism);
    assertEquals("user1", entry.userId);
    assertEquals("myCluster", entry.logicalClusterId);
    assertEquals("no hash", entry.hashedSecret);
    assertEquals("none", entry.hashFunction);
    assertNull(tenantConfig.quotas);
  }

  @Test
  public void shouldLoadFileWithQuotas() throws Exception {
    final String json = "{\"keys\": {\"key1\": {" +
            "\"user_id\": \"user1\"," +
            "\"logical_cluster_id\": \"myCluster\"," +
            "\"sasl_mechanism\": \"PLAIN\"," +
            "\"hashed_secret\": \"no hash\"," +
            "\"hash_function\": \"none\"," +
            "\"is_service_account\": \"true\" }}," +
            "\"quotas\": {\"myCluster\": {" +
            "\"producer_byte_rate\": \"100\"," +
            "\"consumer_byte_rate\": \"200\"," +
            "\"request_percentage\": \"30\"" +
            "}}}";
    Files.write(json, tempFile, Charsets.UTF_8);
    TenantConfig tenantConfig = SecretsLoader.loadFile(tempFile.getPath());
    Map<String, KeyConfigEntry> keys = tenantConfig.apiKeys;
    KeyConfigEntry key1 = new KeyConfigEntry("PLAIN", "no hash", "none", "user1",
        "myCluster", "myCluster", "true");
    assertEquals(1, keys.size());
    assertEquals(key1, keys.get("key1"));

    Map<String, QuotaConfigEntry> quotas = tenantConfig.quotas;
    assertEquals(1, quotas.size());
    QuotaConfigEntry quota = quotas.get("myCluster");
    assertNotNull(quota);
    assertEquals(100, quota.producerByteRate.longValue());
    assertEquals(200, quota.consumerByteRate.longValue());
    assertEquals(30.0, quota.requestPercentage.doubleValue(), 0.0001);

    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", "1"));
    new SecretsLoader.SecretsCacheLoader(tempFile.getPath()).load("key");
    Map<String, String> metricTags = Collections.singletonMap("tenant", "myCluster");
    assertEquals(100.0 / DEFAULT_MIN_PARTITIONS, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, metricTags), 0.001);
    assertEquals(200.0 / DEFAULT_MIN_PARTITIONS, quotaCallback.quotaLimit(ClientQuotaType.FETCH, metricTags), 0.001);
    assertEquals(30.0 / DEFAULT_MIN_PARTITIONS, quotaCallback.quotaLimit(ClientQuotaType.REQUEST, metricTags), 0.001);
  }

  @Test
  public void shouldLoadFileWithDefaultQuotas() throws Exception {
    final String json = "{\"keys\": {\"key1\": {" +
        "\"user_id\": \"user1\"," +
        "\"logical_cluster_id\": \"myCluster\"," +
        "\"sasl_mechanism\": \"PLAIN\"," +
        "\"hashed_secret\": \"no hash\"," +
        "\"hash_function\": \"none\"," +
        "\"is_service_account\": \"false\" }}," +
        "\"quotas\": {\"\": {" +
        "\"producer_byte_rate\": \"1000\"," +
        "\"consumer_byte_rate\": \"2000\"," +
        "\"request_percentage\": \"300\"" +
        "}}}";
    Files.write(json, tempFile, Charsets.UTF_8);
    TenantConfig tenantConfig = SecretsLoader.loadFile(tempFile.getPath());
    Map<String, KeyConfigEntry> keys = tenantConfig.apiKeys;
    assertEquals(1, keys.size());
    KeyConfigEntry key1 = new KeyConfigEntry("PLAIN", "no hash", "none", "user1",
        "myCluster", "myCluster", "false");
    assertEquals(1, keys.size());
    assertEquals(key1, keys.get("key1"));

    Map<String, QuotaConfigEntry> quotas = tenantConfig.quotas;
    assertEquals(1, quotas.size());
    QuotaConfigEntry quota = quotas.get("");
    assertNotNull(quota);
    assertEquals(1000, quota.producerByteRate.longValue());
    assertEquals(2000, quota.consumerByteRate.longValue());
    assertEquals(300.0, quota.requestPercentage.doubleValue(), 0.0001);

    TenantQuotaCallback quotaCallback = new TenantQuotaCallback();
    quotaCallback.configure(Collections.singletonMap("broker.id", "1"));
    new SecretsLoader.SecretsCacheLoader(tempFile.getPath()).load("key");
    Map<String, String> metricTags = Collections.singletonMap("tenant", "myCluster");
    assertEquals(1000.0, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, metricTags), 0.001);
    assertEquals(2000.0, quotaCallback.quotaLimit(ClientQuotaType.FETCH, metricTags), 0.001);
    assertEquals(300, quotaCallback.quotaLimit(ClientQuotaType.REQUEST, metricTags), 0.001);
  }

  @Test
  public void shouldThrowConfigExceptionOnFileNotFound() throws Exception {
    try {
      SecretsLoader.loadFile("/foo/bar/junk/stuff/not_there.92ljlk342");
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("file not found"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnParseError() throws Exception {
    Files.write(":not=valid", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("Error parsing secret file"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnMissingValue() throws Exception {
    Files.write("{\"keys\": {\"key1\": null}}", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("Missing value for key key1"));
    }
  }

  @Test
  public void shouldThrowConfigExceptionOnMissingFields() throws Exception {
    Files.write("{\"keys\": {\"key1\": {}}}", tempFile, Charsets.UTF_8);
    try {
      SecretsLoader.loadFile(tempFile.getPath());
      fail("Did not throw exception");
    } catch (ConfigException e) {
      assertTrue(e.getMessage().contains("user_id field is missing for key key1"));
    }
  }

}