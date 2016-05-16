package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.channel.ChannelFuture;

/**
 * This is for consumer to broker. Only one message can be transported
 * simultaneously.
 */
interface Connection extends Closeable {

    boolean isConnected();

    /**
     * synchronize the protocol packet
     * 
     * @param command
     * @throws IOException
     */
    NSQFrame send(final NSQCommand command) throws TimeoutException;

    /**
     * @param command
     * @return
     */
    ChannelFuture flush(final NSQCommand command);
}
