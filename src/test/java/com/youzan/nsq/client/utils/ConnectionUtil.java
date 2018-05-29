package com.youzan.nsq.client.utils;

import com.youzan.nsq.client.MockedNSQConnectionImpl;
import com.youzan.nsq.client.MockedNSQSimpleClient;
import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by lin on 17/8/11.
 */
public class ConnectionUtil {

    private static Bootstrap bootstrap;
    private static EventLoopGroup eventLoopGroup;

    static{
        //netty setup
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500);
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    public static NSQConnection connect(Address addr, String channel, NSQConfig config) throws InterruptedException, TimeoutException, NSQNoConnectionException, ExecutionException {
        ChannelFuture chFuture = bootstrap.connect(addr.getHost(), addr.getPort());
        final CountDownLatch connLatch = new CountDownLatch(1);
        chFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess())
                    connLatch.countDown();
            }
        });
        connLatch.await(500, TimeUnit.MILLISECONDS);
        Channel ch = chFuture.channel();
        MockedNSQConnectionImpl con1 = new MockedNSQConnectionImpl(0, addr, ch, config);
        con1.setTopic(new Topic(addr.getTopic(), addr.getPartition()));
        NSQSimpleClient simpleClient = new MockedNSQSimpleClient(Role.Consumer, false);
        ch.attr(Client.STATE).set(simpleClient);
        ch.attr(NSQConnection.STATE).set(con1);
        con1.command(Magic.getInstance());
        NSQFrame resp = con1.commandAndGetResponse(null, new Identify(config, addr.isTopicExtend()));
        if (null == resp) {
            throw new IllegalStateException("Bad Identify Response!");
        }
        Thread.sleep(100);
        resp = con1.commandAndGetResponse(null, new Sub(new Topic(addr.getTopic(), addr.getPartition()), channel));
        if (null == resp) {
            throw new IllegalStateException("Bad Identify Response!");
        }
        if (resp.getType() == NSQFrame.FrameType.ERROR_FRAME) {
            throw new IllegalStateException(resp.getMessage());
        }
        con1.subSent();
        Thread.sleep(100);
        con1.command(new Rdy(1));
        Thread.sleep(100);

        return con1;
    }
}
