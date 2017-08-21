package com.youzan.nsq.client.core.pool.producer;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQConnectionImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQNoConnectionException;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.util.IOUtil;
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool factory for {@link oracle.jvm.hotspot.jfr.Producer}
 * <pre>
 * It is a big pool that consists of some sub-pools.
 * Just handle TCP-Connection Object.
 * </pre>
 *
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class KeyedPooledConnectionFactory extends BaseKeyedPooledObjectFactory<Address, NSQConnection> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedPooledConnectionFactory.class);
    private static final Logger PERF_LOG = LoggerFactory.getLogger(KeyedPooledConnectionFactory.class.getName() + ".perf");
    private final AtomicInteger connectionIDGenerator = new AtomicInteger(0);
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap = new Bootstrap();

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
        this.eventLoopGroup = new NioEventLoopGroup(config.getNettyPoolSize());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMillisecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Override
    public NSQConnection create(Address address) throws Exception {
        logger.debug("Begin to create a connection, the address is {}", address);
        long connStart = 0;
        if(PERF_LOG.isDebugEnabled())
            connStart = System.currentTimeMillis();
        final ChannelFuture future = bootstrap.connect(address.getHost(), address.getPort());
        // Wait until the connection attempt succeeds or fails.
        if (!future.awaitUninterruptibly(config.getConnectTimeoutInMillisecond(), TimeUnit.MILLISECONDS)) {
            throw new NSQNoConnectionException(future.cause());
        }
        if(PERF_LOG.isDebugEnabled()) {
            PERF_LOG.debug("Producer pool wait {} milliSec for connection.", System.currentTimeMillis() - connStart);
        }

        final Channel channel = future.channel();
        if (!future.isSuccess()) {
            if (channel != null) {
                channel.close();
            }
            throw new NSQNoConnectionException("Connect " + address + " is wrong.", future.cause());
        }

        long initStart = 0;
        if(PERF_LOG.isDebugEnabled())
            initStart = System.currentTimeMillis();
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
        if(PERF_LOG.isDebugEnabled()) {
            PERF_LOG.debug("Producer pool initialize connection in {} milliSec.", System.currentTimeMillis() - initStart);
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
        if (null != connection && connection.isConnected() && connection.isIdentitySent()) {
            return client.validateHeartbeat(connection);
        }
        logger.warn("NSQConnection {} fails validation.", address);
        return false;
    }

    @Override
    public void destroyObject(Address address, PooledObject<NSQConnection> p) throws Exception {
        NSQConnection connection = p.getObject();
        if (connection.isConnected()) {
            logger.info("Connection pool factory close conn: {}", connection.toString());
            connection.close();
        }
    }


    public void close() {
        if (eventLoopGroup != null && !eventLoopGroup.isShuttingDown()) {
            eventLoopGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS);
        }
    }
}
