// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.store;

import io.confluent.kafka.security.authorizer.provider.ProviderFailedException;

public class MetadataStoreException extends ProviderFailedException {

  private static final long serialVersionUID = 1L;

  public MetadataStoreException(String message) {
    super(message);
  }
}
