package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Consumer extends Client, Closeable {
    @Override
    void start() throws NSQException;

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
