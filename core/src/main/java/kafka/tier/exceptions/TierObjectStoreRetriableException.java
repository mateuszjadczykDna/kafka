/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

import org.apache.kafka.common.errors.RetriableException;

public class TierObjectStoreRetriableException extends RetriableException {

  public TierObjectStoreRetriableException(String message, final Throwable cause) {
    super(message, cause);
  }

  public TierObjectStoreRetriableException(final Throwable cause) {
    super(cause);
  }
}
