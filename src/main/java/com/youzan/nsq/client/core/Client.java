/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static final Logger logger = LoggerFactory.getLogger(Client.class);

    static final AttributeKey<Client> STATE = AttributeKey.valueOf("Client.State");

    static final Random _r = new Random(10000);

    /**
     * For NSQd(data-node).
     */
    static final int _INTERVAL_IN_SECOND = 10;

    void start() throws NSQException;

    /**
     * Receive the frame of NSQ.
     * 
     * @param frame
     * @param conn
     * @throws NSQException
     */
    void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException;

    /**
     * No messages will be sent to the client.
     * 
     * @param conn
     */
    void backoff(final NSQConnection conn);

    /**
     * @return Always it is new.
     */
    ConcurrentSortedSet<Address> getDataNodes();

    void clearDataNode(Address address);

    default void sleep(final int millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Your machine is too busy! Please check it!");
        }
    }

}
