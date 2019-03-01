// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.store;

import org.apache.kafka.common.KafkaException;

public class MetadataStoreException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public MetadataStoreException(String message) {
    super(message);
  }
}
