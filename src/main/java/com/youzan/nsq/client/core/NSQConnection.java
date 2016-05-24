/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.NSQCommand;
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
public class NSQConnection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(NSQConnection.class);

    private boolean havingNegotiation = false;
    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);
    private final Channel channel;
    private final int timeoutInSecond;
    private final long timeoutInMillisecond;

    /**
     * @param channel
     *            It is already connected and alive so far.
     * @param timeoutInSecond
     */
    public NSQConnection(Channel channel, int timeoutInSecond) {
        this.channel = channel;
        if (timeoutInSecond <= 0) {
            timeoutInSecond = 10; // explicit
        }
        this.timeoutInSecond = timeoutInSecond;
        this.timeoutInMillisecond = timeoutInSecond << 10; // ~= * 1024
    }

    /**
     * @return the havingNegotiation
     */
    @Override
    public boolean isHavingNegotiation() {
        return havingNegotiation;
    }

    /**
     * @param havingNegotiation
     *            the havingNegotiation to set
     */
    @Override
    public void setHavingNegotiation(boolean havingNegotiation) {
        this.havingNegotiation = havingNegotiation;
    }

    @Override
    public ChannelFuture command(NSQCommand cmd) {
        return channel.writeAndFlush(cmd);
    }

    @Override
    public NSQFrame commandAndGetResponse(final NSQCommand command) throws TimeoutException {
        final long start = System.currentTimeMillis();
        try {
            long timeout = timeoutInMillisecond - (0L);
            if (!requests.offer(command, timeoutInMillisecond, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Command: " + command + " timedout");
            }

            responses.clear(); // clear
            // write data
            final ChannelFuture future = command(command);

            // wait to get the response
            timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
            if (!future.await(timeout, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Command: " + command + " timedout");
            }

            timeout = timeoutInMillisecond - (start - System.currentTimeMillis());
            final NSQFrame frame = responses.poll(timeout, TimeUnit.MILLISECONDS);
            if (frame == null) {
                throw new TimeoutException("Command: " + command + " timedout");
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
        return channel.isActive();
    }

    @Override
    public void close() {
        if (null != channel) {
            // It is very important!
            channel.attr(Connection.STATE).remove();
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

}
