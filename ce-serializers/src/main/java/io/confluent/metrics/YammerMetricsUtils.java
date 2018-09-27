/*
 * Copyright [2016 - 2016] Confluent Inc.
 */

package io.confluent.metrics;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import java.util.Map;

import io.confluent.metrics.record.ConfluentMetric.YammerGauge;
import io.confluent.metrics.record.ConfluentMetric.YammerHistogram;
import io.confluent.metrics.record.ConfluentMetric.YammerMeter;
import io.confluent.metrics.record.ConfluentMetric.YammerMetricName;
import io.confluent.metrics.record.ConfluentMetric.YammerTimer;

public class YammerMetricsUtils {

  private static final String NAME = ",name=";
  private static final char COMMA = ',';
  private static final char EQUALS = '=';

  public static Map<String, String> extractTags(String metricName) {
    // remove anything prior to the name, which precedes the tags
    int pos = metricName.indexOf(NAME);
    Preconditions.checkArgument(pos >= 0, "invalid metric, missing name");
    pos += NAME.length();

    Map<String, String> tags = Maps.newHashMap();

    int end = metricName.indexOf(COMMA, pos);
    tags.put("name", metricName.substring(pos, end >= 0 ? end : metricName.length()));
    pos = end + 1;

    while (pos > 0) {
      int equals = metricName.indexOf(EQUALS, pos);
      end = metricName.indexOf(COMMA, pos);

      if (equals != -1 && (equals < end || end == -1)) {
        String key = metricName.substring(pos, equals);
        String value = metricName.substring(equals + 1, end > 0 ? end : metricName.length());
        tags.put(key, value);
      }
      pos = end + 1;
    }

    return tags;
  }

  public interface YammerMetric {
    String getName();

    Map<String, String> getTags();

    long longAggregate();

    double doubleAggregate();
  }

  private abstract static class AbstractYammerMetric implements YammerMetric {
    protected final YammerMetricName metricName;

    public AbstractYammerMetric(YammerMetricName metricName) {
      this.metricName = metricName;
    }

    @Override
    public String getName() {
      return metricName.getName();
    }

    @Override
    public Map<String, String> getTags() {
      return YammerMetricsUtils.extractTags(metricName.getMBeanName());
    }

    @Override
    public abstract long longAggregate();

    @Override
    public double doubleAggregate() {
      return 0;
    }
  }

  private abstract static class SuffixedYammerMetric extends AbstractYammerMetric {
    private final String suffix;

    public SuffixedYammerMetric(YammerMetricName metricName, String suffix) {
      super(metricName);
      Preconditions.checkNotNull(suffix, "metric suffix cannot be null");
      this.suffix = suffix;
    }

    @Override
    public String getName() {
      return super.getName() + "-" + suffix;
    }
  }

  public static Iterable<YammerMetric> metricsIterable(Iterable<? extends Message> list) {
    return FluentIterable
        .from(list)
        .transformAndConcat(
            new Function<Message, Iterable<? extends YammerMetric>>() {
              @Override
              public Iterable<? extends YammerMetric> apply(Message input) {
                if (input == null) {
                  throw new IllegalArgumentException("Invalid null input");
                } else if (input instanceof YammerGauge) {
                  return ImmutableList.of(forMessage((YammerGauge) input));
                } else if (input instanceof YammerMeter) {
                  return ImmutableList.of(forMessage((YammerMeter) input));
                } else if (input instanceof YammerHistogram) {
                  return forMessage((YammerHistogram) input);
                } else if (input instanceof YammerTimer) {
                  return ImmutableList.of(forMessage((YammerTimer) input));
                }
                throw new IllegalArgumentException("Unknown message type " + input.getClass());
              }
            }
        );
  }

  private static YammerMetric forMessage(final YammerGauge metric) {
    return new AbstractYammerMetric(metric.getMetricName()) {
      @Override
      public long longAggregate() {
        switch (metric.getNumericValueCase()) {
          case LONGVALUE:
            return metric.getLongValue();
          case DOUBLEVALUE:
            return (long) metric.getDoubleValue();
          default:
            try {
              return Long.parseLong(metric.getValue());
            } catch (NumberFormatException e) {
              try {
                return (long) Double.parseDouble(metric.getValue());
              } catch (NumberFormatException e1) {
                return 0;
              }
            }
        }
      }

      @Override
      public double doubleAggregate() {
        switch (metric.getNumericValueCase()) {
          case LONGVALUE:
            return (double) metric.getLongValue();
          case DOUBLEVALUE:
            return metric.getDoubleValue();
          default:
            try {
              return Double.parseDouble(metric.getValue());
            } catch (NumberFormatException e) {
              return 0;
            }
        }
      }
    };
  }

  private static YammerMetric forMessage(final YammerMeter metric) {
    return new AbstractYammerMetric(metric.getMetricName()) {
      @Override
      public long longAggregate() {
        return metric.getDeltaCount();
      }
    };
  }

  private static YammerMetric forMessage(final YammerTimer metric) {
    return new AbstractYammerMetric(metric.getMetricName()) {
      @Override
      public long longAggregate() {
        return metric.getDeltaCount();
      }
    };
  }

  private static Iterable<YammerMetric> forMessage(final YammerHistogram metric) {
    return ImmutableList.<YammerMetric>of(
        new AbstractYammerMetric(metric.getMetricName()) {
          @Override
          public long longAggregate() {
            return metric.getDeltaCount();
          }
        },
        new SuffixedYammerMetric(metric.getMetricName(), "p95") {
          @Override
          public long longAggregate() {
            return (long) metric.getPercentile95Th();
          }
        },
        new SuffixedYammerMetric(metric.getMetricName(), "p99") {
          @Override
          public long longAggregate() {
            return (long) metric.getPercentile99Th();
          }
        },
        new SuffixedYammerMetric(metric.getMetricName(), "p999") {
          @Override
          public long longAggregate() {
            return (long) metric.getPercentile999Th();
          }
        },
        new SuffixedYammerMetric(metric.getMetricName(), "p50") {
          @Override
          public long longAggregate() {
            return (long) metric.getMedian();
          }
        }
    );
  }

}
