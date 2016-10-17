package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by lin on 16/8/31.
 */
public class PubTrace extends Pub implements HasTraceID{

    private static final Logger logger = LoggerFactory.getLogger(PubTrace.class);
    private byte[] traceId = {0, 0, 0, 0, 0, 0, 0, 0};
    protected UUID id = UUID.randomUUID();

    public PubTrace(Topic topic, byte[] data){
        super(topic, data);
    }

    @Override
    public String getHeader() {
        return String.format("PUB_TRACE %s%s\n", topic.getTopicText(), topic.hasPartition() ?
                SPACE_STR + topic.getPartitionId() : "");
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

    @Override
    public String getID(){
        return this.id.toString();
    }

    public byte[] getTraceId(){
        return this.traceId;
    }

    @Override
    public boolean isTraceIDSet() {
        ByteBuffer buf = ByteBuffer.wrap(this.traceId);
        return buf.getLong() != 0l;
    }

    @Override
    /**
     * update trace Id here, if bytes of current message is initialized, bytes need to be invalidated
     */
    public void updateTraceID(final byte[] traceID) {
        if(this.traceId.length < TRACE_ID_SIZE){
            this.traceId = new byte[TRACE_ID_SIZE];
        }
        System.arraycopy(traceID, 0, this.traceId, 0, TRACE_ID_SIZE);
        //invalidate bytes
        bytes = null;
    }

    public String toString(){
        return this.getID() + "; " + super.toString();
    }
}
