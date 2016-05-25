/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQConnectionImpl implements NSQConnection {
    private static final Logger logger = LoggerFactory.getLogger(NSQConnectionImpl.class);

    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);

    private boolean havingNegotiation = false;

    private final Address address;
    private final Channel channel;
    private final NSQConfig config;
    private final int timeoutInSecond;
    private final long timeoutInMillisecond; // be approximate

    /**
     * @param address
     * @param channel
     * @param config
     */
    public NSQConnectionImpl(Address address, Channel channel, NSQConfig config) {
        this.address = address;
        this.channel = channel;
        this.config = config;

        final int timeout = config.getTimeoutInSecond() <= 0 ? 1 : config.getTimeoutInSecond();
        this.timeoutInSecond = timeout;
        this.timeoutInMillisecond = timeout << 10;
    }

    @Override
    public void init() throws TimeoutException {
        assert address != null;
        assert config != null;
        assert isConnected();
        if (!havingNegotiation) {
            command(Magic.getInstance());
            final NSQCommand ident = new Identify(config);
            final NSQFrame response = commandAndGetResponse(ident);
            if (null == response) {
                throw new IllegalStateException("Bad Identify Response! Close connection!");
            }
            havingNegotiation = true;
        }
        assert havingNegotiation;
    }

    @Override
    public ChannelFuture command(NSQCommand cmd) {
        // Use Netty Pipeline
        return channel.writeAndFlush(cmd);
    }

    @Override
    public NSQFrame commandAndGetResponse(final NSQCommand command) throws TimeoutException {
        final long start = System.currentTimeMillis();
        try {
            long timeout = timeoutInMillisecond - (0L);
            if (!requests.offer(command, timeoutInMillisecond, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }

            responses.clear(); // clear
            // write data
            final ChannelFuture future = command(command);

            // wait to get the response
            timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
            if (!future.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }
            timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
            final NSQFrame frame = responses.poll(timeout, TimeUnit.MILLISECONDS);
            if (frame == null) {
                throw new TimeoutException(
                        "The command is timeout. The command name is : " + command.getClass().getName());
            }

            requests.poll(); // clear
            return frame;
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            logger.error("Thread was interruped, probably shuthing down! Close connection!", e);
        }
        return null;
    }

    @Override
    public boolean isConnected() {
        return channel.isActive() && havingNegotiation;
    }

    @Override
    public void close() {
        if (null != channel) {
            // It is very important!
            channel.attr(NSQConnection.STATE).remove();
            channel.attr(Client.STATE).remove();
            channel.close();
        } else {
            logger.error("No channel be setted?");
        }
    }

    @Override
    public void addResponseFrame(ResponseFrame frame) {
        if (!requests.isEmpty()) {
            try {
                responses.offer(frame, timeoutInSecond, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                close();
                Thread.currentThread().interrupt();
                logger.error("Thread was interruped, probably shuthing down!", e);
            }
        }
    }

    @Override
    public void addErrorFrame(ErrorFrame frame) {
        responses.add(frame);
    }

    /**
     * @return the address
     */
    @Override
    public Address getAddress() {
        return address;
    }

    @Override
    public NSQConfig getConfig() {
        return config;
    }

}
