package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQMessage;

public interface MessageHandler {

    /**
     * 
     * @param message
     * @param exception
     */
    boolean process(NSQMessage message, Exception exception);

}