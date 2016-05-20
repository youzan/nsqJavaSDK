/**
 * Thanks to <href>https://github.com/brainlag/JavaNSQClient</href>
 */
package com.youzan.nsq.client.core;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.Client;
import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NoConnectionException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.util.IOUtil;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
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
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_BACKLOG, Runtime.getRuntime().availableProcessors() - 1); // client-side
        bootstrap.option(ChannelOption.SO_TIMEOUT, config.getTimeoutInSecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Override
    public void start() {
        try {
            start0();
        } catch (Exception e) {
            logger.error("Exception", e);
        }
        // final int size = calcPoolSize();
        // int ok = 0;
        // int retry = 0;
        // Exception last = null;
        // while (ok < size) {
        // retry++;
        // try {
        // create(this.address);
        // ok++;
        // } catch (Exception e) {
        // logger.error("Exception", e);
        // last = e;
        // }
        // if (retry > size * 3) {
        // throw new IllegalStateException("The system cann't create one pool.
        // The last exception:", last);
        // }
        // }
    }

    private void start0() throws Exception {
        create(address);
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
        channel.attr(Client.STATE).set(this);
        channel.attr(ConsumerWorker.STATE).set(this);
        channel.attr(Connection.STATE).set(conn);

        // Send magic
        conn.command(Magic.getInstance());
        // Send the identify. IF ok , THEN return conn. ELSE throws one
        // exception
        final NSQCommand ident = new Identify(config);
        try {
            final NSQFrame response = conn.commandAndGetResponse(ident);
            if (null == response) {
                destoryConnection(conn, channel);
                throw new NSQException("Bad Identify Response!");
            }  // !null => OK. Get the server's negotiation identify
        } catch (final TimeoutException e) {
            destoryConnection(conn, channel);
            throw e;
        }
        return conn;
    }

    @Override
    public void incoming(NSQFrame frame, Connection conn) {
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if ("_heartbeat_".equals(resp)) {
                    conn.command(Nop.getInstance());
                    return;
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                // ErrorCallback ?
                conn.addErrorFrame((ErrorFrame) frame);
                break;
            }
            case MESSAGE_FRAME: {
                process4Client((MessageFrame) frame, conn);
                break;
            }
            default: {
                logger.error("Invalid Frame Type.");
                break;
            }
        }
        return;
    }

    @Override
    public void process4Client(MessageFrame frame, Connection conn) {
        // more exposure to client
        final NSQMessage message = new NSQMessage(frame.getTimestamp(), frame.getAttempts(), frame.getMessageID(),
                frame.getMessageBody());
        // TODO any exception , it will reQueue!
        final boolean successful = handler.process(message);
        if (successful) {
            finish(frame, conn);
        } else {
            reQueue(frame, conn);
        }
    }

    @Override
    public void finish(MessageFrame frame, Connection conn) {
    }

    @Override
    public void reQueue(MessageFrame frame, Connection conn) {
    }

    @Override
    public NSQConfig getConfig() {
        return this.config;
    }

    @Override
    public void sendBackoff() {
    }

    /**
     * Be quiet
     * 
     * @param conn
     * @param channel
     */
    private void destoryConnection(final Connection conn, Channel channel) {
        if (null != conn) {
            IOUtil.closeQuietly(conn);
        }
        if (null != channel) {
            channel.attr(Client.STATE).remove();
            channel.attr(ConsumerWorker.STATE).remove();
            channel.attr(Connection.STATE).remove();
        }
    }

    @Override
    public void close() {
    }

}
