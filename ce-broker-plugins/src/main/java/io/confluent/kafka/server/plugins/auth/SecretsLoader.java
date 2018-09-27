// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.server.plugins.auth.stats.TenantAuthenticationStats;

import org.apache.kafka.common.config.ConfigException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * SecretsLoader periodically re-reads secrets config from the filesystem
 * A filesystem event watcher (http://howtodoinjava.com/java-7/auto-reload-of-configuration-when-any-change-happen/)
 * would be more efficient but this is simpler and good enough
 */
public class SecretsLoader {
  static final String KEY = "key";
  private final LoadingCache<String, Map<String, KeyConfigEntry>> cache;

  public SecretsLoader(String filePath, long refreshMs) {
    this(filePath, refreshMs, Ticker.systemTicker());
  }

  SecretsLoader(String filePath, long refreshMs, Ticker ticker) {
    cache = CacheBuilder.newBuilder()
        .ticker(ticker)
        .expireAfterWrite(refreshMs, TimeUnit.MILLISECONDS)
        .build(new SecretsCacheLoader(filePath));
  }

  public Map<String, KeyConfigEntry> get() throws ExecutionException {
    return cache.get(KEY);
  }

  static Map<String, KeyConfigEntry> loadFile(final String filePath) {
    final Gson gson = new Gson();
    final Type collectionType =
        new TypeToken<Map<String, Map<String, KeyConfigEntry>>>(){}.getType();
    try (final JsonReader reader = new JsonReader(
        new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8)
    )) {
      Map<String, Map<String, KeyConfigEntry>> config = gson.fromJson(reader, collectionType);
      return validateConfig(config);
    } catch (FileNotFoundException e) {
      throw new ConfigException("Config file not found: " + filePath, e);
    } catch (JsonSyntaxException e) {
      throw new ConfigException("Error parsing secret file " + filePath, e);
    } catch (IOException e) {
      throw new ConfigException("Error reading secret file: " + filePath, e);
    }
  }

  static Map<String, KeyConfigEntry> validateConfig(
      Map<String, Map<String, KeyConfigEntry>> config
  ) {
    if (!config.containsKey("keys")) {
      throw new ConfigException("Missing top level \"keys\"");
    }
    Map<String, KeyConfigEntry> keys = config.get("keys");
    for (Map.Entry<String, KeyConfigEntry> entry : keys.entrySet()) {
      validateKeyEntry(entry);
    }
    return keys;
  }

  private static void validateKeyEntry(Map.Entry<String, KeyConfigEntry> entry) {
    KeyConfigEntry v = entry.getValue();
    if (v == null) {
      throw new ConfigException("Missing value for key " + entry.getKey());
    }
    if (isEmpty(v.userId)) {
      throw new ConfigException("user_id field is missing for key " + entry.getKey());
    }
    if (isEmpty(v.logicalClusterId)) {
      throw new ConfigException("logical_cluster_id field is missing for key " + entry.getKey());
    }
    if (isEmpty(v.hashedSecret)) {
      throw new ConfigException("hashed_secret field is missing for key " + entry.getKey());
    }
    if (isEmpty(v.hashFunction)) {
      throw new ConfigException("hash_function field is missing for key " + entry.getKey());
    }
    if (isEmpty(v.saslMechanism)) {
      throw new ConfigException("sasl_mechanism field is missing for key " + entry.getKey());
    }
  }

  private static boolean isEmpty(String str) {
    return str == null || str.isEmpty();
  }

  static class SecretsCacheLoader extends CacheLoader<String, Map<String, KeyConfigEntry>> {
    private final String filePath;

    SecretsCacheLoader(String filePath) {
      this.filePath = filePath;
    }

    @Override
    public Map<String, KeyConfigEntry> load(String key) throws Exception {
      if (!key.equals(KEY)) {
        throw new IllegalArgumentException("Unexpected key: " + key);
      }
      Map<String, KeyConfigEntry> entries = SecretsLoader.loadFile(filePath);
      removeUnusedStatsMbeans(entries);
      return entries;
    }

    private void removeUnusedStatsMbeans(Map<String, KeyConfigEntry> validEntries) {
      Set<MultiTenantPrincipal> validPrincipals = new HashSet<>();
      for (KeyConfigEntry entry : validEntries.values()) {
        validPrincipals.add(new MultiTenantPrincipal(entry.userId,
            new TenantMetadata(entry.logicalClusterId, entry.logicalClusterId)));
      }
      TenantAuthenticationStats.instance().removeUnusedMBeans(validPrincipals);
    }
  }
}
