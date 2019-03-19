/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions;

public class TierPartitionStateIllegalListenerException extends RuntimeException {

    public TierPartitionStateIllegalListenerException(final String message) {
        super(message);
    }

    public TierPartitionStateIllegalListenerException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public TierPartitionStateIllegalListenerException(final Throwable throwable) {
        super(throwable);
    }
}
