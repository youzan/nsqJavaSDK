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

    /**
     * @return the timestamp
     */
    public byte[] getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp
     *            the timestamp to set
     */
    public void setTimestamp(byte[] timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return the attempts
     */
    public byte[] getAttempts() {
        return attempts;
    }

    /**
     * @param attempts
     *            the attempts to set
     */
    public void setAttempts(byte[] attempts) {
        this.attempts = attempts;
    }

    /**
     * @return the messageID
     */
    public byte[] getMessageID() {
        return messageID;
    }

    /**
     * @param messageID
     *            the messageID to set
     */
    public void setMessageID(byte[] messageID) {
        this.messageID = messageID;
    }

    /**
     * @return the messageBody
     */
    public byte[] getMessageBody() {
        return messageBody;
    }

    /**
     * @param messageBody
     *            the messageBody to set
     */
    public void setMessageBody(byte[] messageBody) {
        this.messageBody = messageBody;
    }

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
