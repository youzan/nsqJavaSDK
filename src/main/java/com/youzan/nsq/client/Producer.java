package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
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
     * @throws NSQException
     */
    void publish(byte[] message) throws NSQException;

    /**
     * @see publish(message:byte[])
     * 
     * @param message
     * @throws NSQException
     */
    @Deprecated
    void publish(String message) throws NSQException;

    /**
     * 生产一批的'消息'. 如果一批超过30条,那么SDK会给你按照FIFO顺序的分批(每批30条)发送出去!
     * 因此小于等于30条作为一批的消息,可以作为局部化的有顺序.
     * 
     * Use it to produce some 'messages' sending to MQ. When having too many
     * messages, then split it into 30 messages/batch. When the size <= 30, the
     * messages within a batch is ordered.
     * 
     * @param messages
     * @throws NSQException
     */
    void publishMulti(List<byte[]> messages) throws NSQException;

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
