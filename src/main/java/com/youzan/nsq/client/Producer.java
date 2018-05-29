package com.youzan.nsq.client;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;

import java.io.Closeable;
import java.util.List;

/**
 * Because of too many topics, we create some connections with brokers when actually first time uses.
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Producer extends Client, Closeable {

    /**
     * start producer
     * @throws NSQException
     */
    @Override
    void start() throws NSQException;

    /**
     * start producer with specified topics for initializing associated nsq connection for later publish. the number of
     * nsq connection decided by min idle connection number, refer to {@link NSQConfig#getMinIdleConnectionForProducer()}
     * @param topics topics for initialize
     * @throws NSQException
     */
    void start(String... topics) throws NSQException;

    /**
     * Default UTF-8 Encoding
     * {@link IOUtil#DEFAULT_CHARSET}
     *
     * @param message the client sets it that is be published
     * @param topic   the specified topic name
     * @throws NSQException {@link NSQException}
     */
    @Deprecated
    void publish(String message, String topic) throws NSQException;

    void publish(String message, final Topic topic, Object shardingID) throws NSQException;

    void publish(Message message) throws NSQException;

    /**
     * publish message and return receipt contains target nsqd&topic info for normal publish,
     * and message meta info(internal id, disk offset, disk size, trace id) if message is traced.
     * @param message
     * @return receipt
     * @throws NSQException
     */
    MessageReceipt publishAndGetReceipt(Message message) throws NSQException;

    /**
     * Use it to produce only one 'message' sending to MQ.
     * partition info is not specified in this function,
     * use #{Producer.publish(byte[] messages, Topic topic)}
     *
     * @param message the client sets it that is be published
     * @param topic   the specified topic name
     * @throws NSQException {@link NSQException}
     */
    void publish(byte[] message, String topic) throws NSQException;

    /**
     * publish messages to specified topic
     * @param message message to be sent
     * @param topic the specified topic
     * @throws NSQException {@link NSQException}
     */
    void publish(byte[] message, Topic topic) throws NSQException;

    /**
     * Publish batch messages sending to nsqd in specified batch size, concurrently.
     * Producer tries publishing messages to ALL topic's partitions concurrently.
     * function returns with list of messages which fail to send, if any, otherwise it is consider as NULL
     * @param messages messages to publish
     * @param topic
     * @param batchSize message size
     * @throws NSQException
     * @return list of messages from passin messages which fail to send;
     */
    List<byte[]> publishMulti(List<byte[]> messages, Topic topic, int batchSize) throws NSQException;

    /**
     * publish batch messages to nsqd. This function publish ALL messages to one target nsqd
     * in one MPUB command.
     *
     * @param messages the client sets it that is be published
     * @param topic    the specified topic name
     * @throws NSQException {@link NSQException} if an error occurs.
     */
    void publishMulti(List<byte[]> messages, String topic) throws NSQException;

    /**
     * publish batch messages sending to nsqd. This function publish ALL messages to one target nsqd
     * in one MPUB command.
     *
     * @param messages the client sets it that is be published
     * @param topic    the specified topic name
     * @throws NSQException {@link NSQException} if an error occurs.
     */
    void publishMulti(List<byte[]> messages, Topic topic) throws NSQException;

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();

    /**
     * Return {@link NSQConfig} specified in Producer
     * @return {@link NSQConfig} config
     */
    NSQConfig getConfig();
}
