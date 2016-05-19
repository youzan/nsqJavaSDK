package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQMessage;

/**
 * 
 * It is a callback.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * 
     * @param message
     */
    boolean process(NSQMessage message);

}
