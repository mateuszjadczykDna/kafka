/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.command.record.Command.CommandConfigType;
import io.confluent.command.record.Command.CommandKey;
import io.confluent.command.record.Command.CommandMessage;
import io.confluent.command.record.Command.LicenseInfo;
import io.confluent.serializers.ProtoSerde;

public class LicenseStore {

  private static final Logger log = LoggerFactory.getLogger(LicenseStore.class);
  private static final String KEY_PREFIX = "CONFLUENT_LICENSE";
  private static final CommandKey KEY = CommandKey.newBuilder()
      .setConfigType(CommandConfigType.LICENSE_INFO)
      .setGuid(KEY_PREFIX)
      .build();
  private final String topic;

  public static final String REPLICATION_FACTOR_CONFIG =
      "replication.factor";
  public static final long READ_TO_END_TIMEOUT_MS = 120_000;

  private final KafkaBasedLog<CommandKey, CommandMessage> licenseLog;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<String> latestLicense;
  private final Time time;

  public LicenseStore(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig
  ) {
    this(topic, producerConfig, consumerConfig, topicConfig, Time.SYSTEM);
  }

  // visible for testing
  protected LicenseStore(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig,
      Time time
  ) {
    this.topic = topic;
    this.latestLicense = new AtomicReference<>();
    this.time = time;
    this.licenseLog = setupAndCreateKafkaBasedLog(
        this.topic,
        producerConfig,
        consumerConfig,
        topicConfig,
        this.latestLicense,
        this.time
    );
  }

  // visible for testing
  public LicenseStore(
      String topic,
      AtomicReference<String> latestLicense,
      KafkaBasedLog<CommandKey, CommandMessage> licenseLog,
      Time time
  ) {
    this.topic = topic;
    this.latestLicense = latestLicense;
    this.licenseLog = licenseLog;
    this.time = time;
  }

  // package private for testing
  KafkaBasedLog<CommandKey, CommandMessage> setupAndCreateKafkaBasedLog(
      String topic,
      final Map<String, Object> producerConfig,
      final Map<String, Object> consumerConfig,
      final Map<String, Object> topicConfig,
      AtomicReference<String> latestLicense,
      Time time
  ) {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.putAll(producerConfig);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LicenseKeySerde.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        LicenseMessageSerde.class.getName()
    );
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.putAll(consumerConfig);
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        LicenseKeySerde.class.getName()
    );

    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        LicenseMessageSerde.class.getName()
    );

    String replicationFactorString =
        (String) topicConfig.get(REPLICATION_FACTOR_CONFIG);

    short replicationFactor = replicationFactorString == null
                              ? (short) 3
                              : Short.valueOf(replicationFactorString);

    NewTopic topicDescription = TopicAdmin.defineTopic(topic)
        .compacted()
        .partitions(1)
        .replicationFactor(replicationFactor)
        .build();

    return createKafkaBasedLog(
        topic,
        producerProps,
        consumerProps,
        new ConsumeCallback(latestLicense),
        topicDescription,
        topicConfig,
        time
    );
  }

  private KafkaBasedLog<CommandKey, CommandMessage> createKafkaBasedLog(
      String topic,
      Map<String, Object> producerProps,
      Map<String, Object> consumerProps,
      Callback<ConsumerRecord<CommandKey, CommandMessage>> consumedCallback,
      final NewTopic topicDescription,
      final Map<String, Object> topicConfig,
      Time time
  ) {
    Runnable createTopics = new Runnable() {
      @Override
      public void run() {
        try (TopicAdmin admin = new TopicAdmin(topicConfig)) {
          admin.createTopics(topicDescription);
        }
      }
    };
    return new KafkaBasedLog<>(
        topic,
        producerProps,
        consumerProps,
        consumedCallback,
        time,
        createTopics
    );
  }

  public static class LicenseKeySerde extends ProtoSerde<CommandKey> {
    public LicenseKeySerde() {
      super(CommandKey.getDefaultInstance());
    }
  }

  public static class LicenseMessageSerde extends ProtoSerde<CommandMessage> {
    public LicenseMessageSerde() {
      super(CommandMessage.getDefaultInstance());
    }
  }

  public void start() {
    if (running.compareAndSet(false, true)) {
      log.info("Starting License Store");
      licenseLog.start();
      log.info("Started License Store");
    }
  }

  public void stop() {
    if (running.compareAndSet(true, false)) {
      log.info("Closing License Store");
      licenseLog.stop();
      log.info("Closed License Store");
    }
  }

  public String licenseScan() {
    try {
      licenseLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return latestLicense.get();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error("Failed to read license from Kafka: ", e);
      throw new IllegalStateException(e);
    }
  }

  public void registerLicense(String license) {
    registerLicense(license, null);
  }

  public void registerLicense(String license, org.apache.kafka.clients.producer.Callback callback) {
    CommandMessage licenseMsg = CommandMessage.newBuilder()
        .setLicenseInfo(LicenseInfo.newBuilder().setJwt(license).build())
        .build();
    licenseLog.send(KEY, licenseMsg, callback);
  }

  public static class ConsumeCallback implements
      Callback<ConsumerRecord<CommandKey, CommandMessage>> {
    private final AtomicReference<String> latestLicenseRef;

    ConsumeCallback(AtomicReference<String> latestLicenseRef) {
      this.latestLicenseRef = latestLicenseRef;
    }

    @Override
    public void onCompletion(Throwable error, ConsumerRecord<CommandKey, CommandMessage> record) {
      if (error != null) {
        log.error("Unexpected error in consumer callback for LicenseStore: ", error);
        return;
      }

      if (record.key().getConfigType() == CommandConfigType.LICENSE_INFO) {
        latestLicenseRef.set(record.value().getLicenseInfo().getJwt());
      }
    }
  }
}
