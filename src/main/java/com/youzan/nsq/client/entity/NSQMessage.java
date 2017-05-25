package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.MessageMetadata;
import com.youzan.nsq.client.core.command.Close;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class NSQMessage implements MessageMetadata{
    private static final Logger logger = LoggerFactory.getLogger(Close.class);

    private final byte[] timestamp;
    private final byte[] attempts;
    private final byte[] messageID;
    private final byte[] messageBody;
    final Address address;
    final Integer connectionID; // be sure that is not null

    private IExtContent extContent;

    private long diskQueueOffset = -1L;
    private int diskQueueDataSize = -1;

    /**
     * all the parameters is the NSQ message format!
     *
     * @param timestamp    the raw bytes from the data-node
     * @param attempts     the raw bytes from the data-node
     * @param messageID    the raw bytes from the data-node
     * @param internalID   message internal ID in bytes
     * @param traceID      trace ID in bytes
     * @param messageBody  the raw bytes from the data-node
     * @param address      the address of the message
     * @param connectionID the primary key of the connection
     * @param nextConsumingInSecond time elapse for requeued message to send
     * @param extVerBytes extendable version in bytes
     * @param extBytes    extendable content in bytes
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] internalID, byte[] traceID,
                     byte[] messageBody, Address address, Integer connectionID, int nextConsumingInSecond, byte[] extVerBytes, byte[] extBytes) {
        this.timestamp = timestamp;
        this.attempts = attempts;
        this.messageID = messageID;

        ByteBuffer buf = ByteBuffer.wrap(internalID);
        this.internalID = buf.getLong();
        buf = ByteBuffer.wrap(traceID);
        this.traceID = buf.getLong();

        //extra properties

        this.messageBody = messageBody;
        this.address = address;
        this.connectionID = connectionID;
        // Readable, java.util.Date <= JDK7
        this.datetime = new Date(TimeUnit.NANOSECONDS.toMillis(toLong(timestamp)));
        this.readableAttempts = toUnsignedShort(attempts);
        this.readableMsgID = newHexString(this.messageID);

        this.nextConsumingInSecond = nextConsumingInSecond;

        ExtVer extVer = ExtVer.getExtVersion(extVerBytes);
        extContent = parseExtContent(extVer, extBytes);
    }

    /**
     * NSQMessage constructor, for sub ordered message frame
     * @param timestamp    the raw bytes from the data-node
     * @param attempts     the raw bytes from the data-node
     * @param messageID    the raw bytes from the data-node
     * @param internalID   message internal ID in bytes
     * @param traceID      trace ID in bytes
     * @param messageBody  the raw bytes from the data-node
     * @param address      the address of the message
     * @param connectionID the primary key of the connection
     * @param nextConsumingInSecond time elapse for requeued message to send
     */
    public NSQMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] internalID, byte[] traceID,
                      final byte[] diskQueueOffset, final byte[] diskQueueDataSize, byte[] messageBody, Address address,
                      Integer connectionID, int nextConsumingInSecond, final byte[] extVerBytes, final byte[] extBytes) {
        this(timestamp, attempts, messageID, internalID, traceID, messageBody, address, connectionID, nextConsumingInSecond, extVerBytes, extBytes);

        ByteBuffer buf = ByteBuffer.wrap(diskQueueOffset);
        this.diskQueueOffset = buf.getLong();
        buf = ByteBuffer.wrap(diskQueueDataSize);
        this.diskQueueDataSize = buf.getInt();
    }

    IExtContent parseExtContent(ExtVer extVer, byte[] extBytes) throws IllegalArgumentException {
        switch(extVer) {
            case Ver0x2: {
                String tagFilterStr = new String(extBytes);
                return new DesiredTag(tagFilterStr);
            }
            default: {
                if(extBytes != null) {
                    logger.error("Ext bytes should be empty.");
                    throw new IllegalArgumentException("Ext bytes should be empty.");
                }
                return new NoExtContent();
            }
        }
    }

    IExtContent getExtContent() {
        return this.extContent;
    }

    public DesiredTag getTag() {
        return (DesiredTag)this.extContent;
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

    public long getInternalID() {
        return this.internalID;
    }

    public long getTraceID() {
        return this.traceID;
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
    final Date datetime;
    final int readableAttempts;
    final String readableMsgID;
    protected String readableContent = null;
    private Integer nextConsumingInSecond; // recommend the value is 60 sec
    final long internalID;
    final long traceID;

    /**
     * Default UTF-8 Decoding
     * {@link IOUtil#DEFAULT_CHARSET}
     *
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
     * @param readableContent the readableContent to set
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
     * @param nextConsumingInSecond 1s less-equals the nextConsumingInSecond to set less-equals
     *                              180 days
     * @throws NSQException if an invalid parameter error occurs
     */
    public void setNextConsumingInSecond(Integer nextConsumingInSecond) throws NSQException {
        if (nextConsumingInSecond != null) {
            final int timeout = nextConsumingInSecond.intValue();
            if (timeout < NSQConfig._MIN_NEXT_CONSUMING_IN_SECOND) {
                throw new IllegalArgumentException(
                        "Message.nextConsumingInSecond is illegal. It is too small." + NSQConfig._MIN_NEXT_CONSUMING_IN_SECOND);
            }
            if (timeout > NSQConfig._MAX_NEXT_CONSUMING_IN_SECOND) {
                throw new IllegalArgumentException(
                        "Message.nextConsumingInSecond is illegal. It is too big." + NSQConfig._MAX_NEXT_CONSUMING_IN_SECOND);
            }
        }
        this.nextConsumingInSecond = nextConsumingInSecond;
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

    public long getDiskQueueOffset() {
        return this.diskQueueOffset;
    }

    public int getDiskQueueDataSize() {
        return this.diskQueueDataSize;
    }

    public String toString() {
        String msgStr = "NSQMessage [messageID=" + readableMsgID + ", internalID=" + internalID + ", traceID=" + traceID + ", diskQueueOffset=" + diskQueueOffset + ", diskQueueDataSize=" + diskQueueDataSize + ", datetime=" + datetime + ", readableAttempts="
                + readableAttempts + ", address=" + address + ", connectionID=" + connectionID + "]";
        return msgStr;
    }

    private String metaDataStr;

    @Override
    public String toMetadataStr() {
        if(null == this.metaDataStr) {
            String objStr = getClass().getName() + "@" + Integer.toHexString(hashCode());
            StringBuilder sb = new StringBuilder();
            sb.append(objStr + " meta-data:\n");
            sb.append("\t[dateTime]:\t").append(this.datetime.toString()).append("\n");
            sb.append("\t[attempts]:\t").append(this.readableAttempts).append("\n");
            sb.append("\t[messageID]:\t").append(this.readableMsgID).append("\n");
            sb.append("\t[internalID]:\t").append(this.internalID).append("\n");
            sb.append("\t[traceID]:\t").append(this.traceID).append("\n");
            sb.append("\t[diskQueueOffset]:\t").append(this.diskQueueOffset).append("\n");
            sb.append("\t[diskQueueDataSize]:\t").append(this.diskQueueDataSize).append("\n");
            sb.append("\t[NSQd address]:\t").append(this.address.toString()).append("\n");
            sb.append(objStr + " end.");
            this.metaDataStr = sb.toString();
        }
        return this.metaDataStr;
    }

}
