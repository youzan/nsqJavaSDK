/**
 * 
 */
package com.youzan.nsq.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Stand for one connection underlying TCP
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerWorkerImpl implements ConsumerWorker {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorkerImpl.class);

    private final Address address;
    private final NSQConfig config;
    private final MessageHandler handler;

    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    /**
     * @param config
     * @param handler
     */
    public ConsumerWorkerImpl(Address address, NSQConfig config, MessageHandler handler) {
        this.address = address;
        this.config = config;
        this.handler = handler;

        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Override
    public void incoming(NSQFrame frame) {
    }
}
