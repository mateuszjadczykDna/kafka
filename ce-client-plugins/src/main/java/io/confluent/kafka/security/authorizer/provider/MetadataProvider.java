// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.provider;

import java.io.Closeable;
import org.apache.kafka.common.Configurable;

/**
 * Interface used by providers of metadata, e.g. an Embedded Metadata Server.
 */
public interface MetadataProvider extends Configurable, Closeable {
  /**
   * Returns the name of the provider.
   * @return Provider name
   */
  String providerName();
}
