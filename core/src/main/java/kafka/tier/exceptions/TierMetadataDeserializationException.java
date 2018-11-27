/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

/**
 * {@code TierMetadataDeserializationException} is an exception for cases where metadata
 * belonging to the tier topic cannot be deserialized. This exception is unrecoverable, as a failed
 * materialization will result in data loss.
 */
public class TierMetadataDeserializationException extends RuntimeException {

    public TierMetadataDeserializationException(final String message) {
        super(message);
    }

    public TierMetadataDeserializationException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierMetadataDeserializationException(final Throwable throwable) {
        super(throwable);
    }
}
