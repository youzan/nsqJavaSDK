package com.youzan.nsq.client.core;

import java.io.Closeable;

import com.youzan.nsq.client.network.frame.MessageFrame;

/**
 * connect to one NSQd
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface ConsumerWorker extends Client, Closeable {
    /**
     * Create one connection pool and start working
     */
    void start();

    /**
     * Notify the NSQ-Server to turn off pushing some messages 
     */
    void sendBackoff();

    void process4Client(final MessageFrame frame, final NSQConnection conn);

    void finish(final MessageFrame frame, final NSQConnection conn);

    void reQueue(final MessageFrame frame, final NSQConnection conn);
}
