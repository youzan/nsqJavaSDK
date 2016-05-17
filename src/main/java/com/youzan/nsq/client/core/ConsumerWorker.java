package com.youzan.nsq.client.core;

import org.apache.commons.pool2.KeyedPooledObjectFactory;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.network.frame.NSQFrame;

/**
 * connect to one NSQd
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface ConsumerWorker extends KeyedPooledObjectFactory<Address, Connection> {

    /**
     * create one connection pool and start working
     */
    void start();

    void incoming(NSQFrame frame);
}
