// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

      final Object metricValue = entry.getValue().metricValue();
      if (metricValue instanceof Double) {
        final double value = (Double) metricValue;

        KAFKA_MEASURABLE_BUILDER.clear();
        KAFKA_MEASURABLE_BUILDER.setValue(value);

        KAFKA_METRIC_NAME_BUILDER.clear();
        KAFKA_METRIC_NAME_BUILDER.setGroup(Utils.notNullOrEmpty(metricName.group()));
        KAFKA_METRIC_NAME_BUILDER.setName(Utils.notNullOrEmpty(metricName.name()));
        KAFKA_METRIC_NAME_BUILDER.putAllTags(metricName.tags());

        KAFKA_MEASURABLE_BUILDER.setMetricName(KAFKA_METRIC_NAME_BUILDER.build());

        kafkaMeasurables.add(KAFKA_MEASURABLE_BUILDER.build());
      } else {
        // skip non-measurable metrics
        log.debug("Skipping non-measurable metric {}", metricName.name());
      }
    }

    return kafkaMeasurables;
  }
}
