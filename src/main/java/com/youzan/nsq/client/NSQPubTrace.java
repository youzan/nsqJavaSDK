package com.youzan.nsq.client;

import com.youzan.nsq.client.core.command.HasTraceID;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.TraceInfo;
import com.youzan.nsq.client.network.frame.NSQFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by lin on 16/9/19.
 */
public class NSQPubTrace implements Traceability {

    private static Logger logger = LoggerFactory.getLogger(NSQPubTrace.class);
    //default trace id, which is 0l
    private Object traceIDLock = new Object();
    private final byte[] traceID = new byte[8];
    {
        ByteBuffer buf = ByteBuffer.wrap(traceID);
        buf.putLong(0l);
    }

    private boolean traceOn = false;

    @Override
    public void setTrace(boolean newTraceOn) {
        this.traceOn = newTraceOn;
    }

    @Override
    public boolean isTraceOn() {
        return this.traceOn;
    }

    @Override
    /**
     * update trace ID,
     */
    public void setTraceId(long traceID) {
            byte[] newTraceId = new byte[8];
            ByteBuffer buf = ByteBuffer.wrap(newTraceId);
            buf.putLong(traceID);
            synchronized (traceIDLock) {
                //copy to traceId
                System.arraycopy(newTraceId, 0, this.traceID, 0, this.traceID.length);
            }
    }

    @Override
    /**
     * retrieve current trace id, as 8 byte size array
     */
    public byte[] getTraceId() {
        synchronized(traceIDLock) {
            return this.traceID;
        }
    }

    public void handleFrame(String pubId, NSQFrame frame) {
        TraceInfo traceInfo = new TraceInfo(frame);
        if(logger.isDebugEnabled()) {
            logger.debug("Response for PubTrace {} returns: " + traceInfo.toString(), pubId);
        }
    }

    /**
     * function to insert trace ID into pass in nsq command, impl of insert trace id is delegated to command which
     * implements Interface {@link }
     * @return
     */
    NSQCommand insertTraceID(final NSQCommand cmd){
        if(cmd instanceof HasTraceID){
            ((HasTraceID) cmd).updateTraceID(this.traceID);
            logger.debug("Update trace ID: {} into command: {}", this.traceID, cmd);
        }
        return cmd;
    }
}
