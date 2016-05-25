/**
 * 
 */
package com.youzan.nsq.client.core;

import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.util.AttributeKey;

/**
 * NSQ business processing.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Client {
    public static final AttributeKey<Client> STATE = AttributeKey.valueOf("Client.State");

    /**
     * Receive the frame of NSQ.
     * 
     * @param frame
     * @param conn
     */
    void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException;

    /**
     * No messages will be sent to the client.
     * 
     * @param conn
     * @throws NSQException
     */
    void backoff(final NSQConnection conn) throws NSQException;
}
