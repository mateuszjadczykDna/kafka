/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

public class TierMetadataFatalException extends RuntimeException {

    public TierMetadataFatalException(final String message) {
        super(message);
    }

    public TierMetadataFatalException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierMetadataFatalException(final Throwable throwable) {
        super(throwable);
    }
}
