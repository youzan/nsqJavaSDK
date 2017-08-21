package com.youzan.nsq.client.network.frame;

import com.youzan.nsq.client.MessageMetadata;
import com.youzan.nsq.client.entity.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ResponseFrame extends NSQFrame implements MessageMetadata{
    private static final Logger logger = LoggerFactory.getLogger(ResponseFrame.class);

    @Override
    public FrameType getType() {
        return FrameType.RESPONSE_FRAME;
    }

    @Override
    public String getMessage() {
        return new String(getData(), DEFAULT_CHARSET).trim();
    }

    @Override
    public String toString() {
        return "ResponseFrame: " + this.getMessage();
    }

    @Override
    public String toMetadataStr() {
        String resMsg = getMessage();
        //check if has meta data
        if(resMsg.startsWith("OK") && resMsg.length() > 2){
            byte[] data = getData();

            //internal ID
            byte[] internalIDByte = new byte[8];
            System.arraycopy(data, 2, internalIDByte, 0, 8);
            long internalID = ByteBuffer.wrap(internalIDByte).getLong();

            //traceID
            byte[] traceIDByte = new byte[8];
            System.arraycopy(data, 10, traceIDByte, 0, 8);
            long traceID = ByteBuffer.wrap(traceIDByte).getLong();

            //disk queue offset
            byte[] diskqueueOffsetByte = new byte[8];
            System.arraycopy(data, 18, diskqueueOffsetByte, 0, 8);
            long diskQueueOffset = ByteBuffer.wrap(diskqueueOffsetByte).getLong();

            //disk queue data size
            byte[] diskQueueSizeByte = new byte[4];
            System.arraycopy(data, 26, diskQueueSizeByte, 0, 4);
            int diskQueueSize = ByteBuffer.wrap(diskqueueOffsetByte).getInt();

            StringBuilder sb = new StringBuilder();
            sb.append(this.getClass().toString() + " meta-data:").append("\n");
            sb.append("\t[internalID]:\t").append(internalID).append("\n");
            sb.append("\t[traceID]:\t").append(traceID).append("\n");
            sb.append("\t[diskQueueOffset]:\t").append(diskQueueOffset).append("\n");
            sb.append("\t[diskQueueDataSize]:\t").append(diskQueueSize).append("\n");
            sb.append(this.getClass().toString() + " end.");
            return sb.toString();
        }
        return "No meta data";
    }
}
