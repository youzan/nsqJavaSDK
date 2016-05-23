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

    Producer start();

    /**
     * 生产单条的'消息'
     * 
     * Use it to produce only one 'message' sending to MQ.
     * 
     * @param topic
     * @param message
     * @throws NSQException
     */
    void publish(String topic, byte[] message) throws NSQException;

    /**
     * 生产一批的'消息'. 如果一批超过30条,那么SDK会给你按照FIFO顺序的分批(每批30条)发送出去! *
     * 
     * Use it to produce some 'messages' sending to MQ. When having too many
     * messages, then split it into 30 messages/batch.
     * 
     * @param topic
     * @param messages
     * @throws NSQException
     */
    void publishMulti(String topic, List<byte[]> messages) throws NSQException;

}
