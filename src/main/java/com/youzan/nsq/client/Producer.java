package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Producer extends Client, Closeable {

    Producer start();

    void publish(String topic, byte[] message) throws NSQException, TimeoutException;

    void publishMulti(String topic, List<byte[]> messages);

}
