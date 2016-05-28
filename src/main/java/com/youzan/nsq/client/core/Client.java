/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.Random;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.ConcurrentSortedSet;

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

    static final Random _r = new Random(10000);

    void start() throws NSQException;

    /**
     * Receive the frame of NSQ.
     * 
     * @param frame
     * @param conn
     */
    void incoming(final NSQFrame frame, final NSQConnection conn);

    /**
     * No messages will be sent to the client.
     * 
     * @param conn
     */
    void backoff(final NSQConnection conn);

    /**
     * @return
     */
    ConcurrentSortedSet<Address> getDataNodes();

}
