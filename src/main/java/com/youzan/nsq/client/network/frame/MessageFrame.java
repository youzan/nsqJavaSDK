package com.youzan.nsq.client.network.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MessageFrame extends NSQFrame {
    // *************************************************************************
    // NSQ the message format
    // *************************************************************************
    /**
     * 8-byte : nanosecond timestamp (int64)
     */
    private byte[] timestamp = new byte[8];
    /**
     * 2-byte : (uint16)
     */
    private byte[] attempts = new byte[2];
    /**
     * 16-byte : (hex string encoded in ASCII)
     */
    private byte[] messageID = new byte[16];
    /**
     * N-byte : (binary)
     */
    private byte[] messageBody;

    @Override
    public void setData(byte[] bytes) {
        super.setData(bytes);
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        buf.readBytes(timestamp);
        buf.readBytes(attempts);
        buf.readBytes(messageID);
        messageBody = buf.readBytes(buf.readableBytes()).array();
    }

    @Override
    public FrameType getType() {
        return FrameType.MESSAGE_FRAME;
    }

    @Override
    public String getMessage() {
        throw new UnsupportedOperationException();
    }
}
