package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Close;
import com.youzan.nsq.client.exception.NSQException;
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
     *            the raw bytes from the data-node
     * @param attempts
     *            the raw bytes from the data-node
     * @param messageID
     *            the raw bytes from the data-node
     * @param messageBody
     *            the raw bytes from the data-node
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] messageBody) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.messageID = messageID;
        this.messageBody = messageBody;
        // Readable
        this.datetime = new Date(TimeUnit.NANOSECONDS.toMillis(toLong(timestamp)));
        this.readableAttempts = toUnsignedShort(attempts);
        this.readableMsgID = newHexString(this.messageID);
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
     *                       For client, human message
     * =========================================================================
     */
    private final Date datetime;
    private final int readableAttempts;
    private final String readableMsgID;
    private String readableContent = null;
    private Integer nextConsumingInSecond = null;
    // 1 seconds
    static int _MIN_NEXT_CONSUMING_IN_SECOND = 1;
    // 180 days
    static int _MAX_NEXT_CONSUMING_IN_SECOND = 180 * 24 * 3600;
    // 3 minutes
    static int _DEFAULT_NEXT_CONSUMING_IN_SECOND = 3 * 60;

    /**
     * @return the readableContent
     */
    public String getReadableContent() {
        if (null != readableContent) {
            return readableContent;
        }
        if (messageBody != null && messageBody.length > 0) {
            readableContent = new String(messageBody, IOUtil.DEFAULT_CHARSET);
        } else {
            readableContent = "";
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

    private long toLong(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

    private int toUnsignedShort(byte[] bytes) {
        return (bytes[0] << 8 | bytes[1] & 0xFF) & 0xFFFF;
    }

    /**
     * @return the nextConsumingInSecond
     */
    public Integer getNextConsumingInSecond() {
        return nextConsumingInSecond;
    }

    /**
     * @param nextConsumingInSecond
     *            1s less-equals the nextConsumingInSecond to set less-equals
     *            180 days
     * @throws NSQException
     *             if an invalid parameter error occurs
     */
    public void setNextConsumingInSecond(Integer nextConsumingInSecond) throws NSQException {
        if (nextConsumingInSecond != null) {
            final int timeout = nextConsumingInSecond.intValue();
            if (timeout < _MIN_NEXT_CONSUMING_IN_SECOND) {
                throw new IllegalArgumentException(
                        "Message.nextConsumingInSecond is illegal. It is too small." + _MIN_NEXT_CONSUMING_IN_SECOND);
            }
            if (timeout > _MAX_NEXT_CONSUMING_IN_SECOND) {
                throw new IllegalArgumentException(
                        "Message.nextConsumingInSecond is illegal. It is too big." + _MAX_NEXT_CONSUMING_IN_SECOND);
            }
            this.nextConsumingInSecond = nextConsumingInSecond;
        }
    }

    public String newHexString(byte[] bs) {
        final StringBuffer result = new StringBuffer(bs.length * 2);
        for (byte b : bs) {
            result.append(String.format("%02X", b));
        }
        return result.toString();
    }

    @Override
    public String toString() {
        return "NSQMessage [messageID=" + readableMsgID + ", datetime=" + datetime + ", readableAttempts="
                + readableAttempts + "]";
    }

}
