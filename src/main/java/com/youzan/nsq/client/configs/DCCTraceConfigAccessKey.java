package com.youzan.nsq.client.configs;

/**
 * Tract config access key in DCC config access agent
 * Created by lin on 16/12/14.
 */
public class DCCTraceConfigAccessKey extends AbstractConfigAccessKey {
    private final static String NSQ_TOPIC_TRACE_KEY = "nsq.key.topic.trace";
    private final static String DEFAULT_NSQ_TOPIC_TRACE_PRODUCER_KEY = "topic.trace";

    public DCCTraceConfigAccessKey() {
        super(null);
    }

    @Override
    public String toKey() {
        String key = ConfigAccessAgent.getProperty(NSQ_TOPIC_TRACE_KEY);
        key = null == key ? DEFAULT_NSQ_TOPIC_TRACE_PRODUCER_KEY : key;
        return key;
    }

    public static AbstractConfigAccessKey getInstacne() {
        return new DCCTraceConfigAccessKey();
    }
}
