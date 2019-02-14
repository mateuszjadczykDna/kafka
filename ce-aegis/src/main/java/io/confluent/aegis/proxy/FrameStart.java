// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import org.apache.kafka.common.message.RequestHeaderData;

/**
 * Represents the start of a request or response.
 */
class FrameStart {
    /**
     * The total length of the frame.
     */
    private final int totalLength;

    /**
     * The request header.  This is provided for both requests and responses.
     */
    private final RequestHeaderData header;

    FrameStart(int totalLength, RequestHeaderData header) {
        this.totalLength = totalLength;
        this.header = header;
    }

    public int totalLength() {
        return totalLength;
    }

    public RequestHeaderData header() {
        return header;
    }

    @Override
    public String toString() {
        return "FrameStart(totalLength=" + totalLength +
            ", header=" + header + ")";
    }
}
