package com.youzan.nsq.client.core.command;

/**
 * Created by lin on 16/9/19.
 */
public interface HasTraceID {
    /**
     * update traceID in NSQ command
     * @param traceID
     */
    void updateTraceID(final byte[] traceID);

    /**
     * return current traceID in byte array
     * @return
     */
    byte[] getTraceId();

    /**
     * function to tell if trace id is specified
     * @return
     */
    boolean isTraceIDSet();

    /**
     * return identification for message command
     * @return
     */
    String getID();
}
