package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.TraceConfigAgent;
import com.youzan.nsq.client.core.command.HasTraceID;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.core.command.Pub;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.TraceInfo;
import com.youzan.nsq.client.network.frame.NSQFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by lin on 16/9/19.
 */
class NSQTrace implements Traceability, Comparable<Traceability> {
    private static Logger logger = LoggerFactory.getLogger(NSQTrace.class);

    private final Object agentLock = new Object();
    private TraceConfigAgent agent = TraceConfigAgent.getInstance();
    private UUID id = UUID.randomUUID();

    //default trace id, which is 0l
    private final Object traceIDLock = new Object();
    private final byte[] traceID = new byte[8];
    //default value 0l for trace ID
    {
        ByteBuffer buf = ByteBuffer.wrap(traceID);
        buf.putLong(0L);
    }

    @Override
    public String getID(){
        return this.id.toString();
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

    @Override
    /**
     * check to see if trace switch for pass in topic is on
     */
    public boolean isTraceOn(final Topic topic) {
        TraceConfigAgent agent = this.getAgent();
        if(null == agent) {
            logger.warn("Trace config agent is not initialized.");
            return false;
        }
        return agent.checkTraced(topic);
    }

    private TraceConfigAgent getAgent(){
        if(null == this.agent){
            synchronized (agentLock) {
                if(null == this.agent) {
                    logger.warn("Topic trace agent is not initialized, try fetching...");
                    //try fetch agent again
                    this.agent = TraceConfigAgent.getInstance();
                    if(null == this.agent)
                        logger.warn("Trace agent could not be initialized, pls check config to dcc. Trace functionality is disable untill agent is back.");
                    else
                        logger.info("Trace agent is back online.");
                }
            }
        }
        return this.agent;
    }

    /**
     * print&collect trace info from pub response, as there is no context(mainly topic) to indicate if topic trace is on
     * or not, invoker of this function need to handle that in advance.
     * @param pub Pub command which implements {@Link HasTraceID}
     * @param frame nsq message frame tp parse trace info from.
     */
    void handleFrame(Pub pub, NSQFrame frame) {
        TraceInfo traceInfo = new TraceInfo(frame);
        if(traceLog.isDebugEnabled())
            traceLog.debug("Response for {} {} returns: {}", pub.getClass().toString(), ((HasTraceID) pub).getID(), traceInfo.toString());
    }

    /**
     * handle message from consumer's message incoming.
     * @param message nsq message
     */
    void handleMessage(final NSQMessage message) {
        if(traceLog.isDebugEnabled())
            traceLog.debug("Message received: {}", message.toString());
    }

    /**
     * function to insert trace ID into pass in nsq command, impl of insert trace id is delegated to command which
     * implements Interface {@link HasTraceID}
     * @return NSQ command with trace ID updated
     */
    NSQCommand insertTraceID(final NSQCommand cmd){
        if(cmd instanceof HasTraceID){
            ((HasTraceID) cmd).updateTraceID(this.traceID);
            if(logger.isDebugEnabled())
                logger.debug("Update trace ID: {} into command: {}", this.traceID, cmd);
        }
        return cmd;
    }

    /**
     * log message to trace logger
     * @param msg
     */
    void traceDebug(String msg, Object... objs){
        if(traceLog.isDebugEnabled())
            traceLog.debug(msg, objs);
    }

    @Override
    public int compareTo(Traceability o) {
        return this.getID().compareTo(o.getID());
    }
}
