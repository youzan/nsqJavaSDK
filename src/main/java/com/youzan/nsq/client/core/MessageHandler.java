package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

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
     * @param exception
     */
    boolean process(NSQMessage message, NSQException exception);

}
