/**
 * 
 */
package com.youzan.nsq.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.network.frame.NSQFrame;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerWorkerImpl implements ConsumerWorker {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorkerImpl.class);

    @Override
    public void incoming(NSQFrame frame) {
    }
}
