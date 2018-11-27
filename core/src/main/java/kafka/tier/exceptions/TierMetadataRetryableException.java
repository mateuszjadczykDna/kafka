/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

import java.util.concurrent.CompletionException;

public class TierMetadataRetryableException extends CompletionException {

    public TierMetadataRetryableException(final String message) {
        super(message);
    }

    public TierMetadataRetryableException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierMetadataRetryableException(final Throwable throwable) {
        super(throwable);
    }
}
