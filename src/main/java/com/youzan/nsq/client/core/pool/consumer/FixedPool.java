package com.youzan.nsq.client.core.pool.consumer;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQConnectionImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @deprecated Deprecated as one {@link Address} does not more one topic anymore, after partition is introduced.
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class FixedPool {
    private static final Logger logger = LoggerFactory.getLogger(FixedPool.class);

    private final AtomicInteger connectionIDGenerator = new AtomicInteger(0);
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final int size;


    private final NSQConfig config;
    private final Address address;
    private final Client client;
    private final List<NSQConnection> connections;

    public FixedPool(Address address, int size, Client client, NSQConfig config) {
        this.address = address;
        this.size = size;
        this.client = client;
        this.config = config;
        this.connections = new ArrayList<>(size);
        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = new NioEventLoopGroup(config.getThreadPoolSize4IO());
    }


    public void prepare(boolean isOrdered) throws NSQNoConnectionException {
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMillisecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        for (int i = 0; i < size; i++) {
            final ChannelFuture future = bootstrap.connect(address.getHost(), address.getPort());
            // Wait until the connection attempt succeeds or fails.
            if (!future.awaitUninterruptibly(config.getConnectTimeoutInMillisecond(), TimeUnit.MILLISECONDS)) {
                throw new NSQNoConnectionException(future.cause());
            }
            final Channel channel = future.channel();
            if (!future.isSuccess()) {
                if (channel != null) {
                    channel.close();
                }
                throw new NSQNoConnectionException("Connect " + address + " is wrong.", future.cause());
            }

            final NSQConnection conn = new NSQConnectionImpl(connectionIDGenerator.incrementAndGet(), address, channel,
                    config);
            // Netty async+sync programming
            channel.attr(NSQConnection.STATE).set(conn);
            channel.attr(Client.STATE).set(client);
            channel.attr(Client.ORDERED).set(isOrdered);
            connections.add(conn);
        }
        logger.debug("Having created {} connections for {}", size, address);
    }

    public List<NSQConnection> getConnections() {
        return connections;
    }

    public void close() {
        connections.clear();
        if (!eventLoopGroup.isShuttingDown()) {
            eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS);
        }
    }
}
