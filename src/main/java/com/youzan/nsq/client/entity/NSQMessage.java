package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private final Address address;
    private final Integer connectionID; // be sure that is not null

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
     * @param address
     *            the address of the message
     * @param connectionID
     *            the primary key of the connection
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] messageBody, Address address,
            Integer connectionID) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.messageID = messageID;
        this.messageBody = messageBody;
        this.address = address;
        this.connectionID = connectionID;
        // Readable, java.util.Date <= JDK7
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

    /**
     * @return the address
     */
    public Address getAddress() {
        return address;
    }

    /**
     * @return the connectionID
     */
    public Integer getConnectionID() {
        return connectionID;
    }

    private long toLong(byte[] bytes) {
        final int Long_BYTES = 8;
        final ByteBuffer buffer = ByteBuffer.allocate(Long_BYTES);
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + Arrays.hashCode(attempts);
        result = prime * result + Arrays.hashCode(messageBody);
        result = prime * result + Arrays.hashCode(messageID);
        result = prime * result + ((nextConsumingInSecond == null) ? 0 : nextConsumingInSecond.hashCode());
        result = prime * result + Arrays.hashCode(timestamp);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        NSQMessage other = (NSQMessage) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        if (!Arrays.equals(attempts, other.attempts)) {
            return false;
        }
        if (!Arrays.equals(messageBody, other.messageBody)) {
            return false;
        }
        if (!Arrays.equals(messageID, other.messageID)) {
            return false;
        }
        if (nextConsumingInSecond == null) {
            if (other.nextConsumingInSecond != null) {
                return false;
            }
        } else if (!nextConsumingInSecond.equals(other.nextConsumingInSecond)) {
            return false;
        }
        return Arrays.equals(timestamp, other.timestamp);
    }

    @Override
    public String toString() {
        return "NSQMessage [messageID=" + readableMsgID + ", datetime=" + datetime + ", readableAttempts="
                + readableAttempts + ", address=" + address + ", connectionID=" + connectionID + "]";
    }

}
