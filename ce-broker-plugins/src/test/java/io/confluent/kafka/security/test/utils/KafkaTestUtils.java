// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.test.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTestUtils {

  public static KafkaProducer<String, String> createProducer(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig) {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return new KafkaProducer<>(props);
  }

  public static void sendRecords(KafkaProducer<String, String> producer, String topic, int count)
      throws Throwable {
    List<Future<RecordMetadata>> futures = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key" + i, "value" + i);
      futures.add(producer.send(record));
    }
    for (Future<RecordMetadata> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    }
  }

  public static KafkaConsumer<String, String> createConsumer(
      String bootstrapServers,
      SecurityProtocol securityProtocol,
      String saslMechanism,
      String jaasConfig,
      String consumerGroup) {
    Properties props = new Properties();
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
    props.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    props.setProperty(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaConsumer<>(props);
  }

  public static void consumeRecords(KafkaConsumer<String, String> consumer, String topic, int count)
      throws Exception {
    int received = 0;
    long endTimeMs = System.currentTimeMillis() + 30000;
    consumer.subscribe(Collections.singleton(topic));
    while (received < count && System.currentTimeMillis() < endTimeMs) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      received += records.count();
    }
  }
}
