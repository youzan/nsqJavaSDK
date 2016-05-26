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
     * Business Handler
     * 
     * @param message
     * @return If return false, then reQueue to MQ. If return true, then ACK to
     *         MQ. If any exception occurs, then reQueue to MQ too or giving up
     *         doing a reQueue( SDK will do logging).
     */
    boolean process(NSQMessage message);

}
