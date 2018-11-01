// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter;

import com.yammer.metrics.core.*;
import io.confluent.metrics.record.ConfluentMetric.YammerMetricName;
import io.confluent.metrics.record.ConfluentMetric.YammerHistogram;
import io.confluent.metrics.record.ConfluentMetric.YammerTimer;
import io.confluent.metrics.record.ConfluentMetric.YammerGauge;
import io.confluent.metrics.record.ConfluentMetric.YammerMeter;
import io.confluent.metrics.reporter.YammerMetricsHelper.YammerMetricsResult;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class YammerMetricsTest {
    private YammerMetricsHelper metricsHelper;

    @Before
    public void setUp() throws Exception {
        metricsHelper = new YammerMetricsHelper();
    }

    @Test
    public void simpleGauge() {
        MetricsRegistry registry = new MetricsRegistry();

        MetricName metricName = new MetricName("group1", "type1", "name1", "scope1", "mbean1");
        registry.newGauge(metricName,
                          new Gauge<Integer>() {
                              @Override
                              public Integer value() {
                                  return 100;
                              }
                          });

        YammerMetricsResult result = metricsHelper.collectYammerMetrics(registry, null);

        assertEquals("Should get exactly 1 gauge", 1, result.gauges.size());

        YammerGauge firstGauge = result.gauges.get(0);
        YammerMetricName yammerMetricName = firstGauge.getMetricName();
        assertEquals("Group should match", metricName.getGroup(), yammerMetricName.getGroup());
        assertEquals("Type should match", metricName.getType(), yammerMetricName.getType());
        assertEquals("Name should match", metricName.getName(), yammerMetricName.getName());
        assertEquals("Scope should match", metricName.getScope(), yammerMetricName.getScope());
        assertEquals("MBean should match", metricName.getMBeanName(), yammerMetricName.getMBeanName());
        assertEquals("Value should match", 100, Integer.parseInt(firstGauge.getValue()));
    }

    @Test
    public void simpleMeter() {
        MetricsRegistry registry = new MetricsRegistry();

        MetricName metricName = new MetricName("group1", "type1", "name1", "scope1", "mbean1");
        Meter meter = registry.newMeter(metricName, "meterType", TimeUnit.SECONDS);
        meter.mark(100L);

        YammerMetricsResult result = metricsHelper.collectYammerMetrics(registry, null);

        assertEquals("Should get exactly 1 meter", 1, result.meters.size());

        YammerMeter firstMeter = result.meters.get(0);
        YammerMetricName yammerMetricName = firstMeter.getMetricName();
        assertEquals("Group should match", metricName.getGroup(), yammerMetricName.getGroup());
        assertEquals("Type should match", metricName.getType(), yammerMetricName.getType());
        assertEquals("Name should match", metricName.getName(), yammerMetricName.getName());
        assertEquals("Scope should match", metricName.getScope(), yammerMetricName.getScope());
        assertEquals("MBean should match", metricName.getMBeanName(), yammerMetricName.getMBeanName());
        assertEquals("Count should match", 100L, result.meters.get(0).getCount());
        assertEquals("Delta should match", 0, result.meters.get(0).getDeltaCount());

        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 0, result.meters.get(0).getDeltaCount());

        meter.mark(150);
        meter.mark(175);

        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 325, result.meters.get(0).getDeltaCount());
    }

    @Test
    public void simpleHistogram() {
        MetricsRegistry registry = new MetricsRegistry();

        MetricName metricName = new MetricName("group1", "type1", "name1", "scope1", "mbean1");
        Histogram histogram = registry.newHistogram(metricName, false);
        histogram.update(15L);
        histogram.update(95L);

        YammerMetricsResult result = metricsHelper.collectYammerMetrics(registry, null);

        assertEquals("Should get exactly 1 histogram", 1, result.histograms.size());

        YammerHistogram firstHistogram = result.histograms.get(0);
        YammerMetricName yammerMetricName = firstHistogram.getMetricName();
        assertEquals("Group should match", metricName.getGroup(), yammerMetricName.getGroup());
        assertEquals("Type should match", metricName.getType(), yammerMetricName.getType());
        assertEquals("Name should match", metricName.getName(), yammerMetricName.getName());
        assertEquals("Scope should match", metricName.getScope(), yammerMetricName.getScope());
        assertEquals("MBean should match", metricName.getMBeanName(), yammerMetricName.getMBeanName());
        assertEquals("Count should match", 2L, firstHistogram.getCount());
        assertEquals("Delta should match", 0, firstHistogram.getDeltaCount());
        assertTrue("Max should match", 95.0 == firstHistogram.getMax());
        assertTrue("Min should match", 15.0 == firstHistogram.getMin());

        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 0, result.histograms.get(0).getDeltaCount());

        histogram.update(10);
        histogram.update(15);

        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 2, result.histograms.get(0).getDeltaCount());
    }

    @Test
    public void simpleTimer() {
        MetricsRegistry registry = new MetricsRegistry();

        MetricName metricName = new MetricName("group1", "type1", "name1", "scope1", "mbean1");
        Timer timer = registry.newTimer(metricName, TimeUnit.SECONDS, TimeUnit.SECONDS);
        timer.update(15L, TimeUnit.SECONDS);
        timer.update(95L, TimeUnit.SECONDS);

        YammerMetricsResult result = metricsHelper.collectYammerMetrics(registry, null);

        assertEquals("Should get exactly 1 timer", 1, result.timers.size());

        YammerTimer firstTimer = result.timers.get(0);
        YammerMetricName yammerMetricName = firstTimer.getMetricName();
        assertEquals("Group should match", metricName.getGroup(), yammerMetricName.getGroup());
        assertEquals("Type should match", metricName.getType(), yammerMetricName.getType());
        assertEquals("Name should match", metricName.getName(), yammerMetricName.getName());
        assertEquals("Scope should match", metricName.getScope(), yammerMetricName.getScope());
        assertEquals("MBean should match", metricName.getMBeanName(), yammerMetricName.getMBeanName());
        assertEquals("Count should match", 2L, firstTimer.getCount());
        assertEquals("Delta should match", 0, firstTimer.getDeltaCount());
        assertTrue("Max should match", 95.0 == firstTimer.getMax());
        assertTrue("Min should match", 15.0 == firstTimer.getMin());


        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 0, result.timers.get(0).getDeltaCount());

        timer.update(10, TimeUnit.SECONDS);
        timer.update(20, TimeUnit.SECONDS);
        timer.update(15, TimeUnit.SECONDS);


        result = metricsHelper.collectYammerMetrics(registry, null);
        assertEquals("Delta should match", 3, result.timers.get(0).getDeltaCount());
    }
}
