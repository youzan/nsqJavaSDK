package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.entity.NSQMessage;

import io.netty.util.AttributeKey;

public interface Producer extends Client, Closeable {

    public static final AttributeKey<Client> CONFIG_STATE = AttributeKey.valueOf("ClientConfig.State");

    Producer start();

    /**
     * 
     * @param msg
     */
    void pub(NSQMessage msg);

}
