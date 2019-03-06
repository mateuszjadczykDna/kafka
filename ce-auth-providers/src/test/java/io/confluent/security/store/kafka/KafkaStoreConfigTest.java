// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Test;

public class KafkaStoreConfigTest {

  @Test
  public void testClientConfigs() {
    Properties props = new Properties();
    String bootstrap = "PLAINTEXT://some.host:9092";
    String sslTruststore = "test.truststore.jks";
    props.put(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP, bootstrap);
    props.put(KafkaStoreConfig.REPLICATION_FACTOR_PROP, "1");
    props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
    props.put("confluent.metadata.ssl.truststore.location", sslTruststore);
    props.put("confluent.metadata.ssl.truststore.location", sslTruststore);
    props.put("confluent.metadata.reader.ssl.keystore.location", "reader.keystore.jks");
    props.put("confluent.metadata.writer.ssl.keystore.location", "writer.keystore.jks");
    props.put("confluent.metadata.coordinator.ssl.keystore.location", "coordinator.keystore.jks");
    KafkaStoreConfig config = new KafkaStoreConfig(props);

    Map<String, Object> readerConfigs = config.readerConfigs();
    assertEquals(bootstrap, readerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, readerConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("reader.keystore.jks", readerConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", readerConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(readerConfigs.containsKey("topic.replication.factor"));

    Map<String, Object> writerConfigs = config.writerConfigs();
    assertEquals(bootstrap, writerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, writerConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("writer.keystore.jks", writerConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", writerConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(writerConfigs.containsKey("topic.replication.factor"));

    Map<String, Object> coordinatorConfigs = config.coordinatorConfigs();
    assertEquals(bootstrap, coordinatorConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(sslTruststore, coordinatorConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("coordinator.keystore.jks", coordinatorConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("TLSv1.2", coordinatorConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
    assertFalse(coordinatorConfigs.containsKey("topic.replication.factor"));
  }
}
