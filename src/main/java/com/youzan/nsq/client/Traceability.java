package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lin on 16/9/18.
 */
public interface Traceability {
    static Logger traceLog = LoggerFactory.getLogger("com.youzan.nsq.client.trace");

    String getID();

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

    public boolean isTraceOn(final Topic topic);
}
