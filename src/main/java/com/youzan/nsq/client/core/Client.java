/**
 * 
 */
package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.util.AttributeKey;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Client {
    public static final AttributeKey<Client> STATE = AttributeKey.valueOf("ClientConfig.State");

    NSQConfig getConfig();

    /**
     * Receive the frame of NSQ
     * 
     * @param frame
     * @param conn
     */
    void incoming(final NSQFrame frame, final Connection conn);
}
