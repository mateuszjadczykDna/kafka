// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.external;

import org.apache.kafka.common.Configurable;

public interface ExternalStore<K, V> extends Configurable {

  void start(int generationId);

  void stop(Integer generationId);
}
