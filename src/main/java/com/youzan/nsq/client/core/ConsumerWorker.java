package com.youzan.nsq.client.core;

import java.io.Closeable;

import com.youzan.nsq.client.Client;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.util.AttributeKey;

/**
 * connect to one NSQd
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface ConsumerWorker extends Client, Closeable {

    public static final AttributeKey<ConsumerWorker> STATE = AttributeKey.valueOf("ConsumerWorker.State");

    /**
     * create one connection pool and start working
     */
    void start();

    void incoming(final NSQFrame frame, final Connection conn);
}
