// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerListener<K, V> {

  /**
   * Notification of new record consumed by local reader
   * @param record the record from consumer
   * @param oldValue old value corresponding to record key from local cache
   */
  void onConsumerRecord(ConsumerRecord<K, V> record, V oldValue);
}
