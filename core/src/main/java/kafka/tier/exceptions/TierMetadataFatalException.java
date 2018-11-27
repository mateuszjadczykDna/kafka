/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

import java.util.concurrent.CompletionException;

public class TierMetadataFatalException extends CompletionException {

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
