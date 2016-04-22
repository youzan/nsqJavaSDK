package com.youzan.nsq.client.remoting.handler;

import java.nio.ByteBuffer;

public class NSQMessage {
    // message format defined as 8 byte TS, 2 byte attempts, 16 byte msg ID, N byte body
    public static final int MIN_SIZE_BYTES = 26;
    // nano second
    private long timestamp;
    // really a uint16 but java doesnt do unsigned 
    private int attempts;
    // 16 bytes
    private byte[] messageId;
    private byte[] body;

    public NSQMessage(long timestamp, int attempts, byte[] msgId, byte[] body) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.messageId = msgId;
        this.body = body;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getAttempts() {
        return attempts;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public byte[] getBody() {
        return body;
    }

    public byte[] getBytes() {
        int size = getSize();
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putLong(timestamp);
        bb.putChar((char) attempts);
        bb.put(messageId);
        bb.put(body);
        return bb.array();
    }

    public int getSize() {
        return MIN_SIZE_BYTES + body.length;
    }
}
