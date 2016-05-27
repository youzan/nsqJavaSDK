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
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
