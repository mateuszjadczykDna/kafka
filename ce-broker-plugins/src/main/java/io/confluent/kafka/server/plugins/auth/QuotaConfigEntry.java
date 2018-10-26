// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import com.google.gson.annotations.SerializedName;
import java.util.Objects;

class QuotaConfigEntry {

  @SerializedName("producer_byte_rate") final Long producerByteRate;
  @SerializedName("consumer_byte_rate") final Long consumerByteRate;
  @SerializedName("request_percentage") final Double requestPercentage;

  QuotaConfigEntry(Long producerByteRate, Long consumerByteRate, Double requestPercent) {
    this.producerByteRate = producerByteRate;
    this.consumerByteRate = consumerByteRate;
    this.requestPercentage = requestPercent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QuotaConfigEntry that = (QuotaConfigEntry) o;
    return Objects.equals(producerByteRate, that.producerByteRate)
        && Objects.equals(consumerByteRate, that.consumerByteRate)
        && Objects.equals(requestPercentage, that.requestPercentage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        producerByteRate, consumerByteRate, requestPercentage
    );
  }
}
