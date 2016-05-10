package com.youzan.nsq.client;

import java.io.Closeable;

import com.youzan.nsq.client.entity.NSQMessage;

public interface Producer extends Closeable {

    Producer start();

    /**
     * 
     * @param msg
     */
    void pub(NSQMessage msg);

}
