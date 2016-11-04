package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.ConfigAccessAgent;

/**
 * Created by lin on 16/10/26.
 */
public interface IConfigAccessSubscriber {
    String NSQ_APP_VAL = "nsq.app.val";
    String DEFAULT_NSQ_APP_VAL = "nsq";

    String getDomain();
    String[] getKeys();
    ConfigAccessAgent.IConfigAccessCallback getCallback();
    void subscribe(ConfigAccessAgent subscribeTo);
}
