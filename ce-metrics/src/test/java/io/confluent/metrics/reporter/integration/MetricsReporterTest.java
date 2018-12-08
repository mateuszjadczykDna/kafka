// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import io.confluent.serializers.ProtoSerde;

import static io.confluent.metrics.record.ConfluentMetric.MetricType.BROKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetricsReporterTest extends MetricReporterClusterTestHarness {

    protected final ProtoSerde<ConfluentMetric.MetricsMessage> serdes =
        new ProtoSerde<>(ConfluentMetric.MetricsMessage.getDefaultInstance());
    protected KafkaConsumer<byte[], byte[]> consumer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        consumer = createNewConsumer();
        consumer.subscribe(Collections.singleton(ConfluentMetricsReporterConfig.DEFAULT_TOPIC_CONFIG));
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
        super.tearDown();
    }

    @Test
    public void testMetricsReporter() {
        Result latestResult = null;

        long startMs = System.currentTimeMillis();

        while (System.currentTimeMillis() - startMs < 20000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                latestResult = verify(latestResult == null ? new Result() : latestResult, record);
                if (latestResult.hasAllFields()) {
                    latestResult.verify();
                    return;
                }
            }
        }

        if (latestResult == null)
            fail("No records have been verified");
        else
            fail(String.format("One of the following is false : has Kafka measurable(%b), has Yammer gauge(%b), " +
                    "has Yammer Meter(%b), has Yammer histogram(%b), has Yammer Timer(%b), has CPU Usage(%b)",
                    latestResult.hasKafkaMeasurable, latestResult.hasYammerGauge, latestResult.hasYammerMeter,
                    latestResult.hasYammerHistogram, latestResult.hasYammerTimer, latestResult.hasCpuUsage));
    }

    protected Result verify(Result lastResult, ConsumerRecord<byte[], byte[]> record) {
        Result result = new Result();
        ConfluentMetric.MetricsMessage metricsMessage = serdes.deserialize(record.value());

        assertEquals("metric type should be broker", BROKER, metricsMessage.getMetricType());
        assertTrue("clusterId should be set", !metricsMessage.getClusterId().isEmpty());
        assertTrue("clientId should not be set", metricsMessage.getClientId().isEmpty());
        assertTrue("groupId should not be set", metricsMessage.getGroupId().isEmpty());
        assertEquals("record timestamp should match metric timestamp", metricsMessage.getTimestamp(), record.timestamp());
        verifyBrokerId(metricsMessage.getBrokerId());

        result.hasKafkaMeasurable = lastResult.hasKafkaMeasurable ||
                                    metricsMessage.getKafkaMeasurableCount() > 0;
        result.hasYammerGauge = lastResult.hasYammerGauge ||
                                metricsMessage.getYammerGaugeCount() > 0;
        result.hasYammerMeter = lastResult.hasYammerMeter ||
                                metricsMessage.getYammerMeterCount() > 0;
        result.hasYammerHistogram = lastResult.hasYammerHistogram ||
                                    metricsMessage.getYammerHistogramCount() > 0;
        result.hasYammerTimer = lastResult.hasYammerTimer ||
                                metricsMessage.getYammerTimerCount() > 0;
        result.hasCpuUsage = lastResult.hasCpuUsage ||
                             metricsMessage.getKafkaMeasurableList().stream().anyMatch(km ->
                                     km.getMetricName().getName().equals("CpuUsage"));
        result.systemMetrics = metricsMessage.hasSystemMetrics()
                               ? metricsMessage.getSystemMetrics()
                               : lastResult.systemMetrics;

        return result;
    }

    protected void verifyBrokerId(int brokerId) {
        assertEquals("brokerId should match", 1, brokerId);
    }

    private KafkaConsumer<byte[], byte[]> createNewConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "metric-reporter-consumer");
        // The metric topic may not be there initially. So, we need to refresh metadata more frequently to pick it up once created.
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
        return new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public static class Result {
        boolean hasKafkaMeasurable;
        boolean hasYammerGauge;
        boolean hasYammerMeter;
        boolean hasYammerHistogram;
        boolean hasYammerTimer;
        boolean hasCpuUsage;
        ConfluentMetric.SystemMetrics systemMetrics;

        boolean hasAllFields() {
            return hasKafkaMeasurable && hasYammerFields() && hasCpuUsage && (systemMetrics != null);
        }

        boolean hasYammerFields() {
            return hasYammerGauge && hasYammerMeter && hasYammerHistogram && hasYammerTimer;
        }

        void verify() {
            List<ConfluentMetric.VolumeMetrics> volumes = systemMetrics.getVolumesList();
            assertFalse("Expected to find volumes.", volumes.isEmpty());
            for (ConfluentMetric.VolumeMetrics volume: volumes) {
                assertFalse("The volume name must not be empty.", volume.getName().isEmpty());
                assertTrue("Must have a positive number of total bytes.", volume.getTotalBytes() > 0);
                assertTrue("Must have a positive number of usable bytes.", volume.getUsableBytes() > 0);
                assertTrue("Must have more total bytes than usable bytes.", volume.getTotalBytes() > volume.getUsableBytes());
                assertTrue("Must have at least one log directory.", volume.getLogDirsCount() > 0);
                for (ConfluentMetric.LogDir logDir: volume.getLogDirsList()) {
                    assertFalse("The log directory path must not be empty.", logDir.getPath().isEmpty());
                    assertTrue("The log directory path must start with / (that is, be absolute.)",
                            logDir.getPath().startsWith("/"));
                }
            }
        }
    }
}
