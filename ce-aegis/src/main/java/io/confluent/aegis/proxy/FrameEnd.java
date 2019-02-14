// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

/**
 * Represents the end of a request or response.
 */
class FrameEnd {
    public static final FrameEnd INSTANCE = new FrameEnd();

    @Override
    public String toString() {
        return "FrameEnd()";
    }
}
