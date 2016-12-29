package com.youzan.nsq.client;

import com.youzan.nsq.client.configs.AbstractConfigAccessDomain;
import com.youzan.nsq.client.configs.AbstractConfigAccessKey;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.exception.NSQConfigAccessException;

/**
 * Interface for subscriber to config access agent.
 * Created by lin on 16/10/26.
 */
public interface IConfigAccessSubscriber<T> {
    String NSQ_APP_VAL = "nsq.app.val";
    String DEFAULT_NSQ_APP_VAL = "nsq";

    T subscribe(ConfigAccessAgent subscribeTo, final AbstractConfigAccessDomain domain, final AbstractConfigAccessKey<Role>[] keys, final ConfigAccessAgent.IConfigAccessCallback callback) throws NSQConfigAccessException;
}
