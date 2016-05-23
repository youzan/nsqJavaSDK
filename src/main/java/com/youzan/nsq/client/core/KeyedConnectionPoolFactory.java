/**
 * 
 */
package com.youzan.nsq.client.core;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

/**
 * It is a big pool that consists of some sub-pools.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class KeyedConnectionPoolFactory extends BaseKeyedPooledObjectFactory<Address, Connection> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedConnectionPoolFactory.class);

    private final NSQConfig config;

    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentHashMap<Address, Bootstrap> bootstraps = new ConcurrentHashMap<>();

    public KeyedConnectionPoolFactory(NSQConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getThreadPoolSize4IO());
    }

    @Override
    public Connection create(Address addr) throws Exception {
        final Bootstrap bootstrap;
        if (bootstraps.containsKey(addr)) {
            bootstrap = bootstraps.get(addr);
        } else {
            bootstrap = new Bootstrap();
            bootstraps.putIfAbsent(addr, bootstrap);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.group(eventLoopGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.handler(new NSQClientInitializer());
        }
        // Start the connection attempt.
        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(addr.getHost(), addr.getPort()));

        // Wait until the connection attempt succeeds or fails.
        if (!future.awaitUninterruptibly(config.getTimeoutInSecond(), TimeUnit.SECONDS)) {
            throw new NoConnectionException("Could not connect to server!", future.cause());
        }
        final Channel channel = future.channel();
        if (!future.isSuccess()) {
            throw new NoConnectionException("Could not connect to server!", future.cause());
        }

        final Connection conn = new NSQConnection(channel, config.getTimeoutInSecond());
        // It created Connection !!!
        channel.attr(Connection.STATE).set(conn);
        assert conn != null;
        return conn;
    }

    @Override
    public PooledObject<Connection> wrap(Connection conn) {
        return new DefaultPooledObject<>(conn);
    }

    @Override
    public boolean validateObject(Address addr, PooledObject<Connection> p) {
        final Connection conn = p.getObject();
        if (null != conn) {
            final ChannelFuture future = conn.command(Magic.getInstance());
            return future.awaitUninterruptibly().isSuccess();
        }
        return false;
    }

    @Override
    public void destroyObject(Address addr, PooledObject<Connection> p) throws Exception {
        p.getObject().close();
    }

    public void close() {
        bootstraps.clear();
        if (!eventLoopGroup.isShuttingDown()) {
            Future<?> future = eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS);
        }
    }
}
