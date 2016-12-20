package com.youzan.nsq.client.configs;

import static com.youzan.nsq.client.IConfigAccessSubscriber.DEFAULT_NSQ_APP_VAL;
import static com.youzan.nsq.client.IConfigAccessSubscriber.NSQ_APP_VAL;

/**
 * trace domain in DCC config access agent
 * Created by lin on 16/12/14.
 */
public class DCCTraceConfigAccessDomain extends AbstractConfigAccessDomain {


    public DCCTraceConfigAccessDomain() {
        super(null);
    }

    @Override
    public String toDomain() {
        String domain = ConfigAccessAgent.getProperty(NSQ_APP_VAL);
        return null == domain ? DEFAULT_NSQ_APP_VAL : domain;
    }

    public static AbstractConfigAccessDomain getInstacne() {
        return new DCCTraceConfigAccessDomain();
    }
}
