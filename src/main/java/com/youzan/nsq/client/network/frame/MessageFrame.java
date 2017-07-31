package com.youzan.nsq.client.network.frame;

import com.youzan.nsq.client.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class MessageFrame extends NSQFrame {
    private static final Logger logger = LoggerFactory.getLogger(Message.class);
    /*-
     * =========================================================================
     *                       NSQ the message format
     * =========================================================================
     */
    /**
     * 8-byte : nanosecond timestamp (int64)
     */
    final byte[] timestamp = new byte[8];
    /**
     * 2-byte : (uint16)
     */
    final byte[] attempts = new byte[2];
    /**
     * 16-byte : (hex string encoded in ASCII)
     */
    final byte[] messageID = new byte[16];

    final byte[] internalID = new byte[8];
    final byte[] traceID = new byte[8];

    /**
     * N-byte : (binary)
     */
    byte[] messageBody;

    byte[] extBytes;
    byte[] extVerBytes = new byte[1];


    /*-
     * =========================================================================
     *                       NSQ the message format -- Ending
     * =========================================================================
     */

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

    public byte[] getTractID() {
        return this.traceID;
    }

    public byte[] getInternalID() {
        return this.internalID;
    }

    public byte[] getExtVerBytes() {
        return this.extVerBytes;
    }

    public byte[] getExtBytes() {
        return this.extBytes;
    }

    /**
     * @param messageBody the messageBody to set
     */
    private void setMessageBody(byte[] messageBody) {
        this.messageBody = messageBody;
    }

    public void setData(byte[] bytes, boolean shouldExt) {
        System.arraycopy(bytes, 0, timestamp, 0, 8);
        System.arraycopy(bytes, 8, attempts, 0, 2);

        System.arraycopy(bytes, 10, messageID, 0, 16);
        System.arraycopy(bytes, 10, internalID, 0, 8);
        System.arraycopy(bytes, 18, traceID, 0, 8);

        int messageBodyStart;
        int messageBodySize;
        if(!shouldExt) {
            messageBodyStart = 26;//8 + 2 + 16;
        } else {
            //read ext content & length here
            //version
            System.arraycopy(bytes, 26, extVerBytes, 0, 1);
            int extVer = (int)extVerBytes[0];
            switch (extVer) {
                case 0:
                    messageBodyStart = 27;//8+2+16+1
                    break;
                default:
                    byte[] extBytesLenBytes = new byte[2];
                    //ext content length
                    System.arraycopy(bytes, 27, extBytesLenBytes, 0, 2);
                    int extBytesLen = ByteBuffer.wrap(extBytesLenBytes).getShort();
                    //allocate
                    extBytes = new byte[extBytesLen];
                    System.arraycopy(bytes, 29, extBytes, 0, extBytesLen);
                    messageBodyStart = 29 + extBytesLen;//8 + 2 + 16 + 1 + 2 + extBytesLen;
            }
        }
        messageBodySize = bytes.length - (messageBodyStart);
        messageBody = new byte[messageBodySize];

        System.arraycopy(bytes, messageBodyStart, messageBody, 0, messageBodySize);
    }

    @Override
    @Deprecated
    public void setData(byte[] bytes) {
        final int messageBodySize = bytes.length - (8 + 2 + 16);
        messageBody = new byte[messageBodySize];
        System.arraycopy(bytes, 0, timestamp, 0, 8);
        System.arraycopy(bytes, 8, attempts, 0, 2);

        System.arraycopy(bytes, 10, messageID, 0, 16);
        System.arraycopy(bytes, 10, internalID, 0, 8);
        System.arraycopy(bytes, 18, traceID, 0, 8);
        System.arraycopy(bytes, 26, messageBody, 0, messageBodySize);
    }

    @Override
    public FrameType getType() {
        return FrameType.MESSAGE_FRAME;
    }

    @Override
    public String getMessage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "MessageFrame{" +
                " messageID=" + Arrays.toString(messageID) +
                ", attempts=" + Arrays.toString(attempts) +
                ", timestamp=" + Arrays.toString(timestamp) +
                '}';
    }
}
