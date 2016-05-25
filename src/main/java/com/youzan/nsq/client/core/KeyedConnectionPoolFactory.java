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

import com.youzan.nsq.client.core.command.Nop;
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
 * <pre>
 * It is a big pool that consists of some sub-pools. 
 * Just handle TCP-Connection.
 * </pre>
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class KeyedConnectionPoolFactory extends BaseKeyedPooledObjectFactory<Address, NSQConnection> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedConnectionPoolFactory.class);

    private final NSQConfig config;
    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentHashMap<Address, Bootstrap> bootstraps = new ConcurrentHashMap<>();

    public KeyedConnectionPoolFactory(NSQConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getThreadPoolSize4IO());
    }

    @Override
    public NSQConnection create(Address addr) throws Exception {
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
        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(addr.getHost(), addr.getPort()));

        // Wait until the connection attempt succeeds or fails.
        if (!future.awaitUninterruptibly(config.getTimeoutInSecond(), TimeUnit.SECONDS)) {
            throw new NoConnectionException(future.cause());
        }
        final Channel channel = future.channel();
        if (!future.isSuccess()) {
            throw new NoConnectionException(future.cause());
        }

        final NSQConnection conn = new NSQConnectionImpl(addr, channel, config.getTimeoutInSecond());
        // It created Connection !!!
        channel.attr(NSQConnection.STATE).set(conn);
        assert conn != null;
        return conn;
    }

    @Override
    public PooledObject<NSQConnection> wrap(NSQConnection conn) {
        return new DefaultPooledObject<>(conn);
    }

    @Override
    public boolean validateObject(Address addr, PooledObject<NSQConnection> p) {
        final NSQConnection conn = p.getObject();
        if (null != conn && conn.isConnected()) {
            final ChannelFuture future;
            future = conn.command(Nop.getInstance());
            if (future.awaitUninterruptibly(1, TimeUnit.SECONDS)) {
                return future.isSuccess();
            }
        }
        return false;
    }

    @Override
    public void destroyObject(Address addr, PooledObject<NSQConnection> p) throws Exception {
        p.getObject().close();
    }

    public void close() {
        bootstraps.clear();
        if (!eventLoopGroup.isShuttingDown()) {
            Future<?> future = eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS);
        }
    }
}
