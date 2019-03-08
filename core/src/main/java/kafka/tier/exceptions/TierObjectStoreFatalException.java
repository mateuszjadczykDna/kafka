/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

public class TierObjectStoreFatalException extends RuntimeException {

  public TierObjectStoreFatalException(String message, final Throwable cause) {
    super(message, cause);
  }

  public TierObjectStoreFatalException(final Throwable cause) {
    super(cause);
  }
}
