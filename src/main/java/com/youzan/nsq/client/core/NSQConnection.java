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
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQConnection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(NSQConnection.class);

    public static final AttributeKey<Connection> STATE = AttributeKey.valueOf("Connection.State");

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
        this.timeoutInSecond = timeoutInSecond;
        this.timeoutInMillisecond = timeoutInSecond << 10; // ~= * 1024
    }

    @Override
    public NSQFrame send(final NSQCommand command) throws TimeoutException {
        final long start = System.currentTimeMillis();
        try {
            long timeout = timeoutInMillisecond - (0L);
            if (!requests.offer(command, timeoutInMillisecond, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("Command: " + command + " timedout");
            }

            responses.clear(); // clear
            // write data
            final ChannelFuture future = flush(command);

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
            close(); // broken pipeline
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public ChannelFuture flush(NSQCommand cmd) {
        final ByteBuf buf = channel.alloc().buffer().writeBytes(cmd.getBytes());
        return channel.writeAndFlush(buf);
    }

    @Override
    public boolean isConnected() {
        return channel.isActive();
    }

    @Override
    public void close() {
        if (null != channel) {
            channel.close();
        }
    }
}
