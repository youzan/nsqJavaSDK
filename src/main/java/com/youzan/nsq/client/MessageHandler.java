package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.NSQMessage;

/**
 * It is a callback what is a client processing.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
// @FunctionalInterface
public interface MessageHandler {

    /**
     * Business Processing. Retry 2 times when exceptions in SDK.
     *
     * @param message the concrete message exposing to the client
     */
    void process(NSQMessage message);

}
