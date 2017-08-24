package com.youzan.nsq.client;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.ConnectionManager;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;

/**
 * Try to consume the message using the {@link MessageHandler} again after having a
 * exception.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Consumer extends Client, Closeable {
    /**
     * Initialize some parameters for the consumer
     *
     * @param topics the client wanna subscribe some topics and this API appends
     *               into a topics collector
     */
    void subscribe(String... topics);

    /**
     * Initialize some parameters for the consumer
     *
     * @param topics the client wanna subscribe some topics and this API appends
     *               into a topics collector
     */
    void subscribe(Topic... topics);

    @Override
    void start() throws NSQException;

    void finish(final NSQMessage message) throws NSQException;

    void touch(final NSQMessage message) throws NSQException;

    void setAutoFinish(boolean autoFinish);

    /**
     * set message handler for current consumer, use this function to set up message handler BEFORE consumer starts.
     * Invoking of current throws exception after consumer starts.
     * @param handler
     *              message handler to set up.
     */
    void setMessageHandler(final MessageHandler handler);

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();

    /**
     * Backoff consumption of specified topic. Client indicates nsq server not sending message.
     * Message consumption continues until nsq server hold message flow. Partial partitions backoff is not allowed.
     * @param topic topic consumption to backoff
     */
    void backoff(Topic topic);

    /**
     * Backoff consumption of specified topic, with {@link CountDownLatch} as synchronization.
     * {@link CountDownLatch#countDown()} is invoked once, when
     * 1. after backoff sent to all topic connections.
     * 2. specified topic is already backoff
     *
     * As backoff operation may fails, count down latch should not await eternally
     * @param topic topic to back off
     * @param latch {@link CountDownLatch} count down latch for synchronization, with count 1
     */
    void backoff(Topic topic, CountDownLatch latch);

    /**
     * Resume message consumption of specified backed off topic, with last RDY before topic consumption is backed off.
     *
     * @param topic topic to resume consumption
     */
    void resume(Topic topic);

    void resume(Topic topic, CountDownLatch latch);

    /**
     * Return {@link NSQConfig} specified in Consumer
     * @return {@link NSQConfig} config
     */
    NSQConfig getConfig();

    ConnectionManager getConnectionManager();
}
