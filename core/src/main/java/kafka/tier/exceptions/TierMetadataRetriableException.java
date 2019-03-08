/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

import org.apache.kafka.common.errors.RetriableException;

public class TierMetadataRetriableException extends RetriableException {

    public TierMetadataRetriableException(final String message) {
        super(message);
    }

    public TierMetadataRetriableException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierMetadataRetriableException(final Throwable throwable) {
        super(throwable);
    }
}
