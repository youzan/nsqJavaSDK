package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface Producer extends Client, Closeable {
    @Override
    void start() throws NSQException;

    /**
     * 生产单条的'消息'
     * 
     * Use it to produce only one 'message' sending to MQ.
     * 
     * @param message
     *            the client sets it that is be published
     * @throws NSQException
     *             if an error occurs
     */
    void publish(byte[] message) throws NSQException;

    void publish(byte[] message, String topic) throws NSQException;

    /**
     * @param message
     *            the client sets it that is be published
     * @throws NSQException
     *             if an error occurs
     * @see publish through byte[]
     */
    @Deprecated
    void publish(String message) throws NSQException;

    /**
     * 生产一批的'消息'. 如果一批超过30条,那么SDK会给你按照FIFO顺序的分批(每批30条)发送出去!
     * 因此小于等于30条作为一批的消息,可以作为局部化的有顺序.
     * 
     * Use it to produce some 'messages' sending to MQ. When having too many
     * messages, then split it into 30 messages/batch. When the size less-equals
     * 30, the messages within a batch is ordered.
     * 
     * @param messages
     *            the client sets it that is be published
     * @throws NSQException
     *             if an error occurs
     */
    void publishMulti(List<byte[]> messages) throws NSQException;

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
