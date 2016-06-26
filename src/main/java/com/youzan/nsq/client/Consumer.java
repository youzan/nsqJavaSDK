package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;

/**
 * Try consume the message using the {@code MessageHandler} again after having a
 * exception.
 * 
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
