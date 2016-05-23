package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.core.Client;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Consumer extends Client, Closeable {

    /**
     * Start to consume some messages
     * 
     * @return
     */
    Consumer start();

    /**
     * Perform the action quietly.
     */
    @Override
    void close();
}
