// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.confluent.metrics.record.ConfluentMetric.KafkaMeasurable;
import io.confluent.metrics.record.ConfluentMetric.KafkaMetricName;

public class KafkaMetricsHelper {

  private static final Logger log = LoggerFactory.getLogger(KafkaMetricsHelper.class);

  private static final KafkaMetricName.Builder
      KAFKA_METRIC_NAME_BUILDER =
      KafkaMetricName.newBuilder();
  private static final KafkaMeasurable.Builder
      KAFKA_MEASURABLE_BUILDER =
      KafkaMeasurable.newBuilder();

  private static final boolean SUPPORTS_NON_MEASURABLE_METRICS;

  static {
    Method metricValueMethod = null;
    try {
      metricValueMethod = KafkaMetric.class.getMethod("metricValue");
    } catch (NoSuchMethodException e) {
      // we have an older Kafka < 1.0
    }
    SUPPORTS_NON_MEASURABLE_METRICS = metricValueMethod != null;
  }

  private KafkaMetricsHelper() {
    // static class; can't instantiate
  }

  public static List<KafkaMeasurable> collectKafkaMetrics(
      Map<MetricName, KafkaMetric> metricMap,
      Pattern pattern
  ) {
    List<KafkaMeasurable> kafkaMeasurables = new ArrayList<>();

    for (Map.Entry<MetricName, KafkaMetric> entry : metricMap.entrySet()) {
      MetricName metricName = entry.getKey();
      if (pattern != null && !pattern.matcher(metricName.name()).matches()) {
        continue;
      }

      final KafkaMetric metric = entry.getValue();

      final double value;
      // KafkaMetric.metricValue method was introduced in Kafka 1.0
      // use KafkaMetric.value for backwards compatibility with Kafka 0.11.x
      if (SUPPORTS_NON_MEASURABLE_METRICS) {
        final Object metricValue = metric.metricValue();
        if (metricValue instanceof Double) {
          value = (Double) metricValue;
        } else {
          // skip non-measurable metrics
          log.debug("Skipping non-measurable metric {}", metricName.name());
          continue;
        }
      } else {
        value = metric.value();
      }

      KAFKA_MEASURABLE_BUILDER.clear();
      KAFKA_MEASURABLE_BUILDER.setValue(value);

      KAFKA_METRIC_NAME_BUILDER.clear();
      KAFKA_METRIC_NAME_BUILDER.setGroup(Utils.notNullOrEmpty(metricName.group()));
      KAFKA_METRIC_NAME_BUILDER.setName(Utils.notNullOrEmpty(metricName.name()));
      KAFKA_METRIC_NAME_BUILDER.putAllTags(metricName.tags());
      KAFKA_MEASURABLE_BUILDER.setMetricName(KAFKA_METRIC_NAME_BUILDER.build());

      kafkaMeasurables.add(KAFKA_MEASURABLE_BUILDER.build());
    }

    return kafkaMeasurables;
  }
}
