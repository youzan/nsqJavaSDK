/**
 * 
 */
package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.util.AttributeKey;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Client {
    public static final AttributeKey<Client> STATE = AttributeKey.valueOf("Client.State");

    NSQConfig getConfig();

    /**
     * Do steps , then the TCP-Connection will be NSQ-Connection
     * <ul>
     * <li>Send Magic</li>
     * <li>Send the identify info</li>
     * <ul>
     * 
     * @param conn
     * @throws NSQException
     */
    void identify(final Connection conn) throws NSQException;

    /**
     * Receive the frame of NSQ
     * 
     * @param frame
     * @param conn
     */
    void incoming(final NSQFrame frame, final Connection conn) throws NSQException;
}
