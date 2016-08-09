package com.youzan.nsq.client.core;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.util.IOUtil;
import com.youzan.util.NamedThreadFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 * It is a big pool that consists of some sub-pools.
 * Just handle TCP-Connection Object.
 * </pre>
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class KeyedPooledConnectionFactory extends BaseKeyedPooledObjectFactory<Address, NSQConnection> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedPooledConnectionFactory.class);

    private final AtomicInteger connectionIDGenerator = new AtomicInteger(0);
    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentHashMap<Address, Bootstrap> bootstraps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Address, Long> address_2_bootedTime = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getName(), Thread.NORM_PRIORITY));


    /**
     * Connection/Pool configurations
     */
    private final NSQConfig config;
    /**
     * Because of the protocol initialization
     */
    private final Client client;

    public KeyedPooledConnectionFactory(NSQConfig config, Client client) {
        this.config = config;
        this.client = client;
        this.eventLoopGroup = new NioEventLoopGroup(config.getThreadPoolSize4IO());
        keep();
    }

    private void keep() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // We make a decision that the resources life time should be less than 2 hours
                // Normal max lifetime is 1 hour
                // Extreme max lifetime is 1.5 hours
                final long allow = System.currentTimeMillis() - 3600 * 1000L;
                final Set<Address> expired = new HashSet<>();
                for (Map.Entry<Address, Long> pair : address_2_bootedTime.entrySet()) {
                    if (pair.getValue().longValue() < allow) {
                        expired.add(pair.getKey());
                    }
                }
                for (Address a : expired) {
                    clearDataNode(a);
                }
            }
        }, 30, 30, TimeUnit.MINUTES);
    }

    @Override
    public NSQConnection create(Address address) throws Exception {
        logger.debug("Begin to create a connection, the address is {}", address);
        final Bootstrap bootstrap;
        if (bootstraps.containsKey(address)) {
            bootstrap = bootstraps.get(address);
        } else {
            final Long now = Long.valueOf(System.currentTimeMillis());
            address_2_bootedTime.putIfAbsent(address, now);
            bootstrap = new Bootstrap();
            bootstraps.putIfAbsent(address, bootstrap);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.group(eventLoopGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.handler(new NSQClientInitializer());
        }
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
        try {
            conn.init();
        } catch (Exception e) {
            IOUtil.closeQuietly(conn);
            throw new NSQNoConnectionException("Creating a connection and having a negotiation fails!", e);
        }

        if (!conn.isConnected()) {
            IOUtil.closeQuietly(conn);
            throw new NSQNoConnectionException("Pool failed in connecting to NSQd!");
        }
        return conn;
    }

    @Override
    public PooledObject<NSQConnection> wrap(NSQConnection conn) {
        return new DefaultPooledObject<>(conn);
    }

    @Override
    public boolean validateObject(Address address, PooledObject<NSQConnection> p) {
        final NSQConnection connection = p.getObject();
        // another implementation : use client.heartbeat,or called
        // client.validateConnection
        if (null != connection && connection.isConnected()) {
            return client.validateHeartbeat(connection);
        }
        logger.debug("Validate {} connection! The statue is wrong.", address);
        return false;
    }

    @Override
    public void destroyObject(Address address, PooledObject<NSQConnection> p) throws Exception {
        p.getObject().close();
    }

    private void clearDataNode(Address address) {
        bootstraps.remove(address);
        address_2_bootedTime.remove(address);
        client.clearDataNode(address);
    }

    public void close() {
        bootstraps.clear();
        address_2_bootedTime.clear();
        if (eventLoopGroup != null && !eventLoopGroup.isShuttingDown()) {
            eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS);
        }
    }
}
