/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.List;

import io.confluent.metrics.YammerMetricsUtils.YammerMetric;
import io.confluent.metrics.record.ConfluentMetric.YammerGauge;
import io.confluent.metrics.record.ConfluentMetric.YammerHistogram;
import io.confluent.metrics.record.ConfluentMetric.YammerMeter;
import io.confluent.metrics.record.ConfluentMetric.YammerMetricName;
import io.confluent.metrics.record.ConfluentMetric.YammerTimer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class YammerMetricsUtilsTest {

  @Test
  public void testExtractTags() throws Exception {
    assertEquals(
        ImmutableMap.of(
            "name", "BytesOutPerSec",
            "topic", "_confluent-metrics"
        ),
        YammerMetricsUtils.extractTags("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics")
    );

    assertEquals(
        ImmutableMap.of(
            "name", "BytesOutPerSec",
            "topic", "_confluent-metrics",
            "partition", "1"
        ),
        YammerMetricsUtils.extractTags("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics,partition=1")
    );

    assertEquals(
        ImmutableMap.of(
            "name", "BytesOutPerSec",
            "topic", "",
            "empty", ""
        ),
        YammerMetricsUtils.extractTags("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=,empty=")
    );

    assertEquals(
        ImmutableMap.of(
            "name", "BytesOutPerSec",
            "topic", "_confluent-stuff",
            "validTag", "foo"
        ),
        YammerMetricsUtils.extractTags("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-stuff,invalidTag,validTag=foo")
    );

    assertEquals(
        ImmutableMap.of(
            "name", "cleaner-recopy-percent"
        ),
        YammerMetricsUtils.extractTags("kafka.log:type=LogCleaner,name=cleaner-recopy-percent")
    );

    assertEquals(
        ImmutableMap.of(
            "name", ""
        ),
        YammerMetricsUtils.extractTags("kafka.log:type=Empty,name=")
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetric() throws Exception {
    YammerMetricsUtils.extractTags("");
  }

  @Test
  public void testNumericGauges() throws Exception {
    final YammerMetricName metricName = YammerMetricName.newBuilder()
        .setGroup("dummyGroup")
        .setMBeanName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics")
        .setName("dummyName")
        .build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(YammerMetricsUtils.metricsIterable(
        ImmutableList.of(
            YammerGauge.newBuilder().setMetricName(metricName).setDoubleValue(2.0).build(),
            YammerGauge.newBuilder().setMetricName(metricName).setLongValue(Long.MAX_VALUE).build(),
            YammerGauge.newBuilder().setMetricName(metricName).setValue("1234.5").build()
        )
    ));

    assertEquals(2.0, yammerMetrics.get(0).doubleAggregate(), 0);
    assertEquals(2, yammerMetrics.get(0).longAggregate());

    assertEquals(Long.MAX_VALUE, yammerMetrics.get(1).longAggregate());
    assertEquals((double) Long.MAX_VALUE, yammerMetrics.get(1).doubleAggregate(), 1e3);

    assertEquals(1234, yammerMetrics.get(2).longAggregate());
    assertEquals(1234.5, yammerMetrics.get(2).doubleAggregate(), 0);
  }

  @Test
  public void testMeters() throws Exception {
    final YammerMetricName metricName = YammerMetricName.newBuilder()
        .setGroup("dummyGroup")
        .setMBeanName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics")
        .setName("dummyName")
        .build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(YammerMetricsUtils.metricsIterable(
        ImmutableList.of(
            YammerMeter.newBuilder().setMetricName(metricName).setDeltaCount(100).build()
        )
    ));

    assertEquals(100, yammerMetrics.get(0).longAggregate(), 0);
  }

  @Test
  public void testTimer() throws Exception {
    final YammerMetricName metricName = YammerMetricName.newBuilder()
        .setGroup("dummyGroup")
        .setMBeanName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics")
        .setName("dummyName")
        .build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(YammerMetricsUtils.metricsIterable(
        ImmutableList.of(
            YammerTimer.newBuilder().setMetricName(metricName).setDeltaCount(100).build()
        )
    ));

    assertEquals(100, yammerMetrics.get(0).longAggregate(), 0);
  }

  @Test
  public void testHistograms() throws Exception {
    final YammerMetricName metricName = YammerMetricName.newBuilder()
        .setGroup("dummyGroup")
        .setMBeanName("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=_confluent-metrics")
        .setName("dummyName")
        .build();

    final List<YammerMetric> yammerMetrics = Lists.newArrayList(YammerMetricsUtils.metricsIterable(
        ImmutableList.of(
            YammerHistogram.newBuilder()
                .setMetricName(metricName)
                .setDeltaCount(100)
                .setPercentile95Th(95)
                .setPercentile99Th(99)
                .setPercentile999Th(999)
                .setMedian(50)
                .build()
        )
    ));

    assertEquals(100, yammerMetrics.get(0).longAggregate(), 0);
    assertEquals(95, yammerMetrics.get(1).longAggregate(), 0);
    assertTrue(yammerMetrics.get(1).getName().endsWith("p95"));
    assertEquals(99, yammerMetrics.get(2).longAggregate(), 0);
    assertTrue(yammerMetrics.get(2).getName().endsWith("p99"));
    assertEquals(999, yammerMetrics.get(3).longAggregate(), 0);
    assertTrue(yammerMetrics.get(3).getName().endsWith("p999"));
    assertEquals(50, yammerMetrics.get(4).longAggregate(), 0);
    assertTrue(yammerMetrics.get(4).getName().endsWith("p50"));
  }


  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMetric() throws Exception {
      YammerMetricsUtils.extractTags("kafka.log:type=Dummy");
  }
}
