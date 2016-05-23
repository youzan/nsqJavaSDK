package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

/**
 * This is for consumer to broker. Only one message can be transported
 * simultaneously.
 */
public interface Connection extends Closeable {

    public static final AttributeKey<Connection> STATE = AttributeKey.valueOf("Connection.State");

    boolean isConnected();

    boolean isIdentified();

    void setIdentified(boolean identified);

    /**
     * synchronize the protocol packet
     * 
     * @param command
     * @throws IOException
     */
    NSQFrame commandAndGetResponse(final NSQCommand command) throws TimeoutException;

    /**
     * @param command
     * @return
     */
    ChannelFuture command(final NSQCommand command);

    /**
     * @param frame
     */
    void addResponseFrame(ResponseFrame frame);

    /**
     * @param frame
     */
    void addErrorFrame(ErrorFrame frame);
}
