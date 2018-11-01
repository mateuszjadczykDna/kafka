// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter.integration;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import io.confluent.metrics.record.ConfluentMetric;
import kafka.utils.TestUtils;
import scala.collection.JavaConversions;

import static io.confluent.metrics.record.ConfluentMetric.MetricType.CONSUMER;
import static io.confluent.metrics.record.ConfluentMetric.MetricType.PRODUCER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientMetricReporterTest extends MetricsReporterTest {

    private Producer<String, String> testProducer;
    private static final String TOPIC = "testtopic";
    private KafkaConsumer<byte[], byte[]> testConsumer;

    @Before
    public void setUp() throws Exception {
        enableMetricsReporterOnBroker = false;
        super.setUp();
        TestUtils.createTopic(this.zkClient, TOPIC, 2, 1, JavaConversions.asScalaBuffer(servers).seq(), new Properties());
        testProducer = createProducer();
        produceTestData(testProducer, TOPIC, 50);
        testConsumer = createConsumer();
        consumeTestData(testConsumer, TOPIC);
    }

    @After
    public void tearDown() throws Exception {
        testProducer.close();
        testConsumer.close();
        super.tearDown();
    }

    @Test
    @Override
    public void testMetricsReporter() {
        long startMs = System.currentTimeMillis();
        int producerMetricCount = 0;
        int consumerMetricCount = 0;

        int kafkaMeasurables = 0;
        int yammerMetrics = 0;
        while (System.currentTimeMillis() - startMs < 20000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(200);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                ConfluentMetric.MetricsMessage metricsMessage = serdes.deserialize(record.value());
                if (metricsMessage.getMetricType() == CONSUMER) {
                    assertEquals("groupId should be set", "test-group", metricsMessage.getGroupId());
                    assertEquals("clientId should be set", "test-consumer", metricsMessage.getClientId());
                    consumerMetricCount++;
                } else if (metricsMessage.getMetricType() == PRODUCER) {
                    assertEquals("clientId should be set", "test-producer", metricsMessage.getClientId());
                    producerMetricCount++;
                } else {
                    fail("only clients should produce metrics");
                }

                assertEquals("brokerId should be -1", -1, metricsMessage.getBrokerId());
                assertTrue("clusterId should be set", !metricsMessage.getClusterId().isEmpty());
                kafkaMeasurables += metricsMessage.getKafkaMeasurableCount();
                yammerMetrics += metricsMessage.getYammerGaugeCount();
                yammerMetrics += metricsMessage.getYammerMeterCount();
                yammerMetrics += metricsMessage.getYammerHistogramCount();
                yammerMetrics += metricsMessage.getYammerTimerCount();
            }
        }
        assertTrue("Clients should have a KafkaMeasurable metric", kafkaMeasurables > 0);
        assertEquals("Clients should not have a Yammer metric", 0, yammerMetrics);
        assertTrue("should receive metrics from consumer", consumerMetricCount > 0);
        assertTrue("should receive metrics from producer", producerMetricCount > 0);
    }

    private Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        injectMetricReporterProperties(properties, brokerList);
        return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
    }

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
        injectMetricReporterProperties(properties, brokerList);
        return new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private void produceTestData(Producer<String, String> producer, String topic, int numRecords) {
        for (int i = 0; i < numRecords; i++)
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
    }

    private void consumeTestData(KafkaConsumer<byte[], byte[]> consumer, String topic) {
        consumer.subscribe(Collections.singleton(topic));
        ConsumerRecords<byte[], byte[]> records = consumer.poll(200);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            // pass
        }
    }
}
