package com.youzan.nsq.client.network.frame;

import java.nio.ByteBuffer;

/**
 * Created by lin on 16/9/26.
 */
public class OrderedMessageFrame extends MessageFrame{
    /**
     * 8-byte : disk queue offset
     */
    final private byte[] diskQueueOffset = new byte[8];
    /**
     * 4-byte : disk queue data size
     */
    final private byte[] diskQueueDataSize = new byte[4];

    @Override
    public void setData(byte[] bytes, boolean shouldExt) {
        System.arraycopy(bytes, 0, timestamp, 0, 8);
        System.arraycopy(bytes, 8, attempts, 0, 2);

        //Sub Ordered incoming extra info, disk queue offset & disk queue data size
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

        System.arraycopy(bytes, messageBodyStart, diskQueueOffset, 0, 8);
        System.arraycopy(bytes, messageBodyStart + 8, diskQueueDataSize, 0, 4);

        messageBodySize = bytes.length - (messageBodyStart + 8 + 4);
        messageBody = new byte[messageBodySize];

        System.arraycopy(bytes, messageBodyStart + 8 + 4, messageBody, 0, messageBodySize);
    }

    @Override
    public void setData(byte[] bytes) {
        //capability of message array bytes.length - (8 + 2 + 8 + 4 + 16)
        final int messageBodySize = bytes.length - (38);
        messageBody = new byte[messageBodySize];
        System.arraycopy(bytes, 0, timestamp, 0, 8);
        System.arraycopy(bytes, 8, attempts, 0, 2);
        //Sub Ordered incoming extra info, disk queue offset & disk queue data size
        System.arraycopy(bytes, 10, messageID, 0, 16);
        System.arraycopy(bytes, 10, internalID, 0, 8);
        System.arraycopy(bytes, 18, traceID, 0, 8);

        System.arraycopy(bytes, 26, diskQueueOffset, 0, 8);
        System.arraycopy(bytes, 34, diskQueueDataSize, 0, 4);


        System.arraycopy(bytes, 38, messageBody, 0, messageBodySize);
    }

    /**
     * function to get diskQueueOffset of current msg, diskqueue has meaning only when message contains advanced info of
     * SUB
     * @return diskQueueOffSet (int 64) in byte[]
     */
    public byte[] getDiskQueueOffset(){
        return this.diskQueueOffset;
    }

    /**
     * function to get diskQueueDataSize of current msg, disk queue dat size has meaning only when message contains
     * advanced info of SUB
     * @return diskQueueDataSize (int 64) in byte[]
     */
    public byte[] getDiskQueueDataSize(){
        return this.diskQueueDataSize;
    }
}
