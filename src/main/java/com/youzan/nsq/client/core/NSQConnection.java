package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.io.IOException;
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
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface NSQConnection extends Closeable {

    public static final AttributeKey<NSQConnection> STATE = AttributeKey.valueOf("Connection.State");

    Address getAddress();

    NSQConfig getConfig();

    /**
     * If any client wants to use my connection, then the client need to pass
     * itself into me because of the usage of Netty.
     * 
     * @throws Exception
     * 
     */
    void init() throws Exception;

    /**
     * Netty. 异步/同步, 转换的上下文设置. Do it for encoder/decoder.
     * 
     * @param client
     */
    void setClient(Client client);

    /**
     * @return
     */
    boolean isConnected();

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

    /**
     * Never throws any exception to the client. It is quiet.
     */
    @Override
    void close();
}
