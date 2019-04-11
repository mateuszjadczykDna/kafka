package org.apache.kafka.common.raft;

import org.apache.kafka.common.KafkaException;

public class LogTruncationException extends KafkaException {

    public LogTruncationException(String message) {
        super(message);
    }
}
