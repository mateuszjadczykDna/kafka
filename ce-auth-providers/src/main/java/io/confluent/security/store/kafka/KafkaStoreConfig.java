// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.security.store.kafka;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

public class KafkaStoreConfig extends AbstractConfig {

  public static final String PREFIX = "confluent.metadata.";

  public static final String TOPIC_CREATE_TIMEOUT_PROP = "confluent.metadata.topic.create.timeout.ms";
  private static final int TOPIC_CREATE_TIMEOUT_DEFAULT = 60000;
  private static final String TOPIC_CREATE_TIMEOUT_DOC = "The number of milliseconds to wait for"
      + " metadata topic to be created during start up.";

  public static final String REFRESH_TIMEOUT_PROP = "confluent.metadata.refresh.timeout.ms";
  private static final int REFRESH_TIMEOUT_DEFAULT = 60000;
  private static final String REFRESH_TIMEOUT_DOC = "The number of milliseconds to wait for cache"
      + " to be refreshed after a write completes successfully.";

  public static final String BOOTSTRAP_SERVERS_PROP = PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(BOOTSTRAP_SERVERS_PROP, Type.LIST,
            Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        .define(TOPIC_CREATE_TIMEOUT_PROP, Type.INT, TOPIC_CREATE_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, TOPIC_CREATE_TIMEOUT_DOC)
        .define(REFRESH_TIMEOUT_PROP, Type.INT, REFRESH_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, REFRESH_TIMEOUT_DOC);
  }

  public final Duration topicCreateTimeout;
  public final Duration refreshTimeout;
  private final String bootstrapServers;

  public KafkaStoreConfig(Map<?, ?> props) {
    super(CONFIG, props);

    topicCreateTimeout = Duration.ofMillis(getInt(TOPIC_CREATE_TIMEOUT_PROP));
    refreshTimeout = Duration.ofMillis(getInt(REFRESH_TIMEOUT_PROP));
    this.bootstrapServers = Utils.join(getList(BOOTSTRAP_SERVERS_PROP), ",");
  }

  public Map<String, Object> readerConfigs() {
    Map<String, Object> configs = originalsWithPrefix(PREFIX + "reader.");
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "__metadata_reader_group");
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "__metadata_reader");

    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return configs;
  }

  public Map<String, Object> writerConfigs() {
    Map<String, Object> configs = originalsWithPrefix(PREFIX + "writer.");
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "__metadata_writer");

    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.RETRIES_CONFIG, "0");
    configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    return configs;
  }

  public Map<String, Object> coordinatorConfigs() {
    Map<String, Object> configs = originalsWithPrefix(PREFIX + "coordinator.");
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, "__metadata_coordinator_group");
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "__metadata_coordinator");

    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // Consumer will be created with deserializer overrides
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return configs;
  }

  @Override
  public String toString() {
    return String.format("%s: %n\t%s", getClass().getName(), Utils.mkString(values(), "", "", "=", "%n\t"));
  }

  public static void main(String[] args) throws Exception {
    try (PrintStream out = args.length == 0 ? System.out
        : new PrintStream(new FileOutputStream(args[0]), false, StandardCharsets.UTF_8.name())) {
      out.println(CONFIG.toHtmlTable());
      if (out != System.out) {
        out.close();
      }
    }
  }
}
