// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.metrics.record.ConfluentMetric.KafkaMeasurable;
import io.confluent.metrics.record.ConfluentMetric.KafkaMetricName;

import static org.junit.Assert.assertEquals;

public class KafkaMetricsTest {

    @Test
    public void simpleKafkaMeasurable() {
        Metrics metrics = new Metrics();
        Map<String, String> tags = new HashMap<>();
        tags.put("tag", "value");

        MetricName metricName = metrics.metricName("name1", "group1", tags);
        metrics.addMetric(metricName, new Measurable() {
            public double measure(MetricConfig config, long now) {
                return 100.0;
            }
        });

        List<KafkaMeasurable> result = KafkaMetricsHelper.collectKafkaMetrics(metrics.metrics(), null);

        assertEquals("Should get exactly 2 Kafka measurables since Metrics always includes a count measurable", 2, result.size());
        for (KafkaMeasurable kafkaMeasurable : result) {
            KafkaMetricName kafkaMetricName = kafkaMeasurable.getMetricName();
            // skip the built-in count metric
            if ("count".equals(kafkaMetricName.getName()))
                continue;

            assertEquals("Name should match", metricName.name(), kafkaMetricName.getName());
            assertEquals("Group should match", metricName.group(), kafkaMetricName.getGroup());
            assertEquals("Tags should match", metricName.tags(), kafkaMetricName.getTagsMap());
            assertEquals("Value should match", 100.0, kafkaMeasurable.getValue(), 0.0);
        }
    }

    @Test
    public void testNonMeasurableMetrics() {
        Metrics metrics = new Metrics();
        MetricName metricName = metrics.metricName("m1", "measurableGroup");
        metrics.addMetric(metricName, new Measurable() {
            public double measure(MetricConfig config, long now) {
                return 100.0;
            }
        });

        metricName = metrics.metricName("m2", "nonMeasurableGroup");
        metrics.addMetric(metricName, new Gauge<String>() {
            @Override
            public String value(MetricConfig config, long now) {
                return "hello";
            }
        });

        List<KafkaMeasurable> result = KafkaMetricsHelper.collectKafkaMetrics(metrics.metrics(), null);

        assertEquals(
            ImmutableList.of("count", "m1"),
            Lists.transform(
                result,
                new Function<KafkaMeasurable, String>() {
                    @Override
                    public String apply(KafkaMeasurable kafkaMeasurable) {
                        return kafkaMeasurable.getMetricName().getName();
                    }
                }
            )
        );
    }
}
