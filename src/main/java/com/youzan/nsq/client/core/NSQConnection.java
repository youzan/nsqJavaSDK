package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

/**
 * <pre>
 * NSQ Connection Definition.
 * This is underlying Netty Pipeline with decoder and encoder.
 * </pre>
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public interface NSQConnection extends Closeable {

    public static final AttributeKey<NSQConnection> STATE = AttributeKey.valueOf("Connection.State");

    Address getAddress();

    NSQConfig getConfig();

    boolean isConnected();

    /**
     * If any client wants to use my connection, then the client need to pass
     * itself into me before calling init(this method) because of the usage of
     * Netty.
     * 
     * @throws TimeoutException
     *             a timeout error
     */
    void init() throws TimeoutException;

    /**
     * Synchronize the protocol packet
     * 
     * @param command
     *            a {@code NSQCommand}
     * @return a {@code NSQFrame} after send a request
     * @throws TimeoutException
     *             a timed out error
     */
    NSQFrame commandAndGetResponse(final NSQCommand command) throws TimeoutException;

    ChannelFuture command(final NSQCommand command);

    void addResponseFrame(ResponseFrame frame);

    void addErrorFrame(ErrorFrame frame);

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
