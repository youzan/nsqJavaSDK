package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.NSQMessage;

/**
 * It is a callback what is a client processing.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * Business Processing. Retry 2 times when exceptions in SDK.
     * 
     * @param message
     */
    void process(NSQMessage message);

}
