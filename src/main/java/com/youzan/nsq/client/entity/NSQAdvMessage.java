package com.youzan.nsq.client.entity;

import java.nio.ByteBuffer;

/**
 * NSQ adcanced message, for sub message contains extra trace info:
 * diskQueueOffset (long) and diskQueueDataSize (int)
 * Created by lin on 16/9/11.
 */
public class NSQAdvMessage extends NSQMessage {
    private long diskQueueOffset;
    private int diskQueueDataSize;

    /**
     * all the parameters is the NSQ message format!
     *
     * @param timestamp    the raw bytes from the data-node
     * @param attempts     the raw bytes from the data-node
     * @param messageID    the raw bytes from the data-node
     * @param messageBody  the raw bytes from the data-node
     * @param address      the address of the message
     * @param connectionID the primary key of the connection
     */
    public NSQAdvMessage(byte[] timestamp, byte[] attempts, byte[] messageID, byte[] internalID, byte[] traceID, final byte[] diskQueueOffset, final byte[] diskQueueDataSize, byte[] messageBody, Address address, Integer connectionID) {
        super(timestamp, attempts, messageID, internalID, traceID, messageBody, address, connectionID);

        ByteBuffer buf = ByteBuffer.wrap(diskQueueOffset);
        this.diskQueueOffset = buf.getLong();
        buf = ByteBuffer.wrap(diskQueueDataSize);
        this.diskQueueDataSize = buf.getInt();
    }

    public long getDiskQueueOffset(){
        return this.diskQueueOffset;
    }

    public int getDiskQueueDataSize(){
        return this.diskQueueDataSize;
    }

    public String toString(){
        String msgStr = "NSQMessage [messageID=" + readableMsgID + ", internalID=" + internalID + ", traceID=" + traceID + ", diskQueueOffset=" + diskQueueOffset + ", diskQueueDataSize=" + diskQueueDataSize + ", datetime=" + datetime + ", readableAttempts="
                + readableAttempts + ", address=" + address + ", connectionID=" + connectionID + "]";
        return msgStr;
    }
}
