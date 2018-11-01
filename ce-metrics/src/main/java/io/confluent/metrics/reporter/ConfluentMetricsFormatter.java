// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.PrintStream;
import java.util.Properties;

import io.confluent.metrics.record.ConfluentMetric.MetricsMessage;
import io.confluent.serializers.ProtoSerde;
import kafka.common.MessageFormatter;

public class ConfluentMetricsFormatter implements MessageFormatter {

  private final ProtoSerde<MetricsMessage>
      serdes =
      new ProtoSerde<>(MetricsMessage.getDefaultInstance());
  private boolean printTopic;
  private boolean printPartition;
  private boolean printTs;
  private boolean printKey;
  private String fieldSeparator;
  private String lineSeparator;

  @Override
  public void init(Properties props) {
    printTopic = getBoolean(props, "print.topic", false);
    printPartition = getBoolean(props, "print.partition", false);
    printTs = getBoolean(props, "print.timestamp", false);
    printKey = getBoolean(props, "print.key", false);
    lineSeparator = props.getProperty("line.separator", "\n");
    fieldSeparator = props.getProperty("field.separator", "\t");
  }

  private boolean getBoolean(
      Properties props,
      String name,
      boolean defaultValue
  ) {
    if (props.containsKey(name)) {
      return Boolean.parseBoolean(props.getProperty(name).trim());
    }
    return defaultValue;
  }

  @Override
  public void writeTo(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      PrintStream output
  ) {
    if (printTopic) {
      output.append(consumerRecord.topic());
      output.append(fieldSeparator);
    }
    if (printPartition) {
      output.append(Integer.toString(consumerRecord.partition()));
      output.append(fieldSeparator);
    }
    if (printTs) {
      output.append(Long.toString(consumerRecord.timestamp()));
      output.append(fieldSeparator);
    }
    if (printKey) {
      output.append("null");
      output.append(fieldSeparator);
    }
    output.append(getValueString(consumerRecord.topic(), consumerRecord.value(), serdes));
    output.append(lineSeparator);
  }

  private String getValueString(
      String topic,
      byte[] valueBytes,
      ProtoSerde<MetricsMessage> serdes
  ) {
    String valueStr = "null";
    if (valueBytes != null) {
      MetricsMessage value = serdes.deserialize(valueBytes);
      valueStr = serdes.toJson(value);
    }
    return valueStr;
  }

  @Override
  public void close() {
  }

}
