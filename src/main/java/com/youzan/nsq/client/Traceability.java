package com.youzan.nsq.client;

/**
 * Created by lin on 16/9/18.
 */
public interface Traceability {
    /**
     * turn on/off traceability
     * @param flag
     */
    void setTrace(boolean flag);

    /**
     * indicate if traceability is on
     * @return
     */
    boolean isTraceOn();

    /**
     * set trace ID for traceability, trace id will be converted to 8 byte size
     * @param traceID
     */
    void setTraceId(long traceID);

    /**
     * return tracce id in 8 byte size
     * @return
     */
    byte[] getTraceId();
}
