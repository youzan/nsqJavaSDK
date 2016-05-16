package com.youzan.nsq.client.core;

import com.youzan.nsq.client.network.frame.NSQFrame;

/**
 * connect to one NSQd
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface ConsumerWorker {
    void incoming(NSQFrame frame);
}
