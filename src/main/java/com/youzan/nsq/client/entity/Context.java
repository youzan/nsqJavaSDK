package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Context for set/get extra properties like tracing ID
 * Created by lin on 17/5/4.
 */
public class Context {
    private final static Logger log = LoggerFactory.getLogger(Context.class);

    //UUID for tracing
    private long traceID = 0L;

    public void setTraceID(long traceID) {
        this.traceID = traceID;
    }

    public long getTraceID() {
        return this.traceID;
    }
}
