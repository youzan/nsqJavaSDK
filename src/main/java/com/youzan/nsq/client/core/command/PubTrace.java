package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by lin on 16/8/31.
 */
public class PubTrace extends Pub{

    private static final Logger logger = LoggerFactory.getLogger(PubTrace.class);
    private byte[] traceId = {0, 0, 0, 0, 0, 0, 0, 0};
    protected UUID id = UUID.randomUUID();

    public PubTrace(Message msg){
        super(msg);
        if(msg.getTraceID() != 0L) {
            //update traceID
            ByteBuffer buf = ByteBuffer.allocate(TRACE_ID_SIZE);
            buf.putLong(msg.getTraceID());
            byte[] newTraceID = buf.array();
            System.arraycopy(newTraceID, 0, this.traceId, 0, TRACE_ID_SIZE);
        }
    }

    @Override
    public String getHeader() {
        return String.format("PUB_TRACE %s%s\n", topic.getTopicText(), this.getPartitionStr());
    }

    @Override
    public byte[] getBytes(){
        if(null == bytes){
            byte[] header = this.getHeader().getBytes(NSQCommand.DEFAULT_CHARSET);
            //extra 4 byte for traceID and message size value
            int msgSize = header.length + MSG_SIZE;
            //set it as array[0], as we need length 0 for size calculation
            byte[] traceIDBytes = this.getTraceId();
            msgSize += TRACE_ID_SIZE;
            byte[] body = this.getBody().get(0);
            msgSize += body.length;
            ByteBuffer buf = ByteBuffer.allocate(msgSize);

            buf.put(header)
                    .putInt(( TRACE_ID_SIZE ) + body.length)
                    .put(traceIDBytes)
                    .put(body);
            bytes = buf.array();
        }
        return bytes;
    }

    public byte[] getTraceId(){
        return this.traceId;
    }

    public String toString(){
        return this.getTraceId() + "; " + super.toString();
    }
}
