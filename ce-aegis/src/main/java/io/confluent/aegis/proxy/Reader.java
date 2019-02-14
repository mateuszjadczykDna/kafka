// Copyright 2018, Confluent

package io.confluent.aegis.proxy;

import io.confluent.aegis.common.ByteBufAccessor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;

abstract class Reader extends ChannelInboundHandlerAdapter {
    private final static int MAX_REQUEST_SIZE = 256 * 1024 * 1024;

    private Logger log;

    private int frameLength;

    private int remaining;

    private CompositeByteBuf buf;

    private ByteBufAccessor bufferAccessor;

    Reader(Logger log) {
        this.log = log;
        reset();
    }

    Logger log() {
        return log;
    }

    void setLogger(Logger log) {
        this.log = log;
    }

    int frameLength() {
        return frameLength;
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object input) throws Exception {
        ByteBuf in = (ByteBuf) input;
        if (this.buf == null) {
            this.buf = ctx.alloc().compositeBuffer();
            this.bufferAccessor = new ByteBufAccessor(this.buf);
        }
        this.buf.addComponent(true, in);
        if (this.remaining <= 0) {
            int wireLength = 0;
            try {
                wireLength = bufferAccessor.readInt();
            } catch (IndexOutOfBoundsException e) {
                // We need to wait for more bytes before we can read the frame length.
                return;
            }
            if (log.isTraceEnabled()) {
                log.trace("Read frame length {}", wireLength);
            }
            if (wireLength <= 0) {
                throw new RuntimeException("Received a frame length of " + wireLength + ", which is too short.");
            } else if (wireLength > MAX_REQUEST_SIZE) {
                throw new RuntimeException("Received a frame length of " + wireLength + ", which is too long.");
            }
            this.frameLength = wireLength;
            this.remaining = wireLength;
        }
        tryReadStructuredData(ctx);
    }

    abstract void tryReadStructuredData(ChannelHandlerContext ctx) throws Exception;

    final boolean readMessage(ApiMessage message, short version) {
        int prevReaderIndex = buf.readerIndex();
        try {
            message.read(bufferAccessor, version);
        } catch (IndexOutOfBoundsException e) {
            if (log.isTraceEnabled()) {
                log.trace("Unable to read type {} message.", message.apiKey(), e);
            }
            buf.readerIndex(prevReaderIndex);
            return false;
        }
        int bytesRead = buf.readerIndex() - prevReaderIndex;
        remaining -= bytesRead;
        if (remaining < 0) {
            throw new IndexOutOfBoundsException("Message " + message +
                " extended " + -remaining + " byte(s) past the frame length.");
        }
        if (log.isTraceEnabled()) {
            log.trace("Read type {} message in {} byte(s).", message.apiKey(), bytesRead);
        }
        return true;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        reset(); // Free buffered memory when the channel is unregistered.
        ctx.fireChannelUnregistered();
    }

    void reset() {
        this.frameLength = 0;
        this.remaining = 0;
        if (this.buf != null) {
            this.buf.release();
            this.buf = null;
            this.bufferAccessor = null;
        }
    }
}
