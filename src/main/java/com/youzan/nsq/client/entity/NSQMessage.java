package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Close;
import com.youzan.util.IOUtil;

public class NSQMessage {
    private static final Logger logger = LoggerFactory.getLogger(Close.class);

    private final byte[] timestamp;
    private final byte[] attempts;
    private final byte[] messageID;
    private final byte[] messageBody;

    /**
     * all the parameters is the NSQ message format!
     * 
     * @param timestamp
     * @param attempts
     * @param messageID
     * @param messageBody
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] messageBody) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.messageID = messageID;
        this.messageBody = messageBody;
        // Readable
        this.datetime = new Date(TimeUnit.NANOSECONDS.toMillis(toLong(timestamp)));
        this.readableAttempts = toUnsignedShort(attempts);
        this.readableMsgID = new String(messageID, IOUtil.ASCII);
    }

    /**
     * @return the timestamp
     */
    public byte[] getTimestamp() {
        return timestamp;
    }

    /**
     * @return the attempts
     */
    public byte[] getAttempts() {
        return attempts;
    }

    /**
     * @return the messageID
     */
    public byte[] getMessageID() {
        return messageID;
    }

    /**
     * @return the messageBody
     */
    public byte[] getMessageBody() {
        return messageBody;
    }

    /*-
     * =========================================================================
     *                       For client , human message
     * =========================================================================
     */
    private final Date datetime;
    private final int readableAttempts;
    private final String readableMsgID;
    private String readableContent;

    /**
     * @return the readableContent
     */
    public String getReadableContent() {
        if (null != readableContent) {
            return readableContent;
        }
        if (messageBody != null && messageBody.length > 0) {
            readableContent = new String(messageBody, IOUtil.DEFAULT_CHARSET);
        }
        return readableContent;
    }

    /**
     * @param readableContent
     *            the readableContent to set
     */
    private void setReadableContent(String readableContent) {
        this.readableContent = readableContent;
    }

    /**
     * @return the datetime
     */
    public Date getDatetime() {
        return datetime;
    }

    /**
     * @return the readableAttempts
     */
    public int getReadableAttempts() {
        return readableAttempts;
    }

    /**
     * @return the readableMsgID
     */
    public String getReadableMsgID() {
        return readableMsgID;
    }

    private long toLong(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

    private int toUnsignedShort(byte[] bytes) {
        return (bytes[0] << 8 | bytes[1] & 0xFF) & 0xFFFF;
    }

    @Override
    public String toString() {
        return readableMsgID;
    }
}
