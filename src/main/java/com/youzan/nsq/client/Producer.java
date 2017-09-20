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
     * @Deprecated Method not implemented.
     * 生产一批的'消息'. <br>
     * 如果一批超过30条,那么SDK会给你按照FIFO顺序的分批(每批30条)发送出去! <br>
     * 因此小于等于30条作为一批的消息,可以作为局部化的有顺序.
     * <p>
     * Use it to produce some 'messages' sending to MQ. When having too many
     * messages, then split it into 30 messages/batch. When the size less-equals
     * 30, the messages within a batch is ordered.
     *
     * @param messages the client sets it that is be published
     * @param topic    the specified topic name
     * @throws NSQException {@link NSQException} if an error occurs.
     */
    void publishMulti(List<byte[]> messages, String topic) throws NSQException;

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
