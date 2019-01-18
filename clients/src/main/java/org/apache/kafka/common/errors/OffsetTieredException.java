/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.common.errors;

public class OffsetTieredException extends ApiException {
    private static final long serialVersionUID = 1L;

    public OffsetTieredException(String message) {
        super(message);
    }

    public OffsetTieredException(String message, Throwable cause) {
        super(message, cause);
    }
}
