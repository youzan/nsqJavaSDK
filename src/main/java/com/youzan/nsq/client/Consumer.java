package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

/**
 * Try consume the message using the {@code MessageHandler} again after having a
 * exception.
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Consumer extends Client, Closeable {
    /**
     * initialize some parameters for the consumer
     * 
     * @param topics
     *            the client wanna subscribe some topics and this API appends
     *            into a topics collector
     */
    void subscribe(String... topics);

    @Override
    void start() throws NSQException;

    void finish(NSQMessage message) throws NSQException;

    void setAutoFinish(boolean autoFinish);

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
