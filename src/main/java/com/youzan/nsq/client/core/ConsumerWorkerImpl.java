/**
 * Thanks to <href>https://github.com/brainlag/JavaNSQClient</href>
 */
package com.youzan.nsq.client.core;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Stand for one connection pool(client->one broker) underlying TCP.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerWorkerImpl implements ConsumerWorker {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorkerImpl.class);

    /**
     * NSQd(DataNode) address
     */
    private final Address address;
    private final NSQConfig config;
    private final MessageHandler handler;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;

    /**
     * @param address
     * @param config
     * @param handler
     */
    public ConsumerWorkerImpl(Address address, NSQConfig config, MessageHandler handler) {
        this.address = address;
        this.config = config;
        this.handler = handler;

        final int size = calcPoolSize();
        this.eventLoopGroup = new NioEventLoopGroup(size);

        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Override
    public void start() {
        final int size = calcPoolSize();
        int ok = 0;
        int retry = 0;
        Exception last = null;
        while (ok < size) {
            retry++;
            try {
                create(this.address);
                ok++;
            } catch (Exception e) {
                logger.error("Exception", e);
                last = e;
            }
            if (retry > size * 3) {
                throw new IllegalStateException("The system cann't create one pool. The last exception:", last);
            }
        }
    }

    /**
     * @param config
     * @return
     */
    private int calcPoolSize() {
        /* pool size */
        final int size;
        if (config.isOrdered()) {
            size = 1;
        } else {
            final int tmp = config.getConnectionPoolSize();
            size = tmp <= 0 ? Runtime.getRuntime().availableProcessors() * 2 : tmp;
        }
        return size;
    }

    public Connection create(final Address addr) throws Exception {
        // Create one connection and connect to the broker
        // Start the connection attempt.
        final ChannelFuture future = this.bootstrap.connect(new InetSocketAddress(addr.getHost(), addr.getPort()));

        // Wait until the connection attempt succeeds or fails.
        if (!future.awaitUninterruptibly(config.getTimeoutInSecond(), TimeUnit.SECONDS)) {
            throw new NoConnectionException("Could not connect to server", future.cause());
        }
        Channel channel = future.channel();
        if (!future.isSuccess()) {
            throw new NoConnectionException("Could not connect to server", future.cause());
        }

        final Connection conn = new NSQConnection(channel, this.config.getTimeoutInSecond());
        channel.attr(Connection.STATE).set(conn);
        channel.attr(ConsumerWorker.STATE).set(this);
        // Send magic
        conn.flush(Magic.getInstance());
        // Send the identify. IF ok , THEN return conn. ELSE throws one
        // exception
        final NSQFrame response = conn.send(new Identify(this.config));
        return conn;
    }

    @Override
    public void incoming(Connection conn, NSQFrame msg) {
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void close() {
    }

}
