package com.youzan.nsq.client.core;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

/**
 * NSQ Connection Definition
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface NSQConnection extends Closeable {

    public static final AttributeKey<NSQConnection> STATE = AttributeKey.valueOf("Connection.State");

    void init();

    /**
     * @return
     */
    Address getAddress();

    /**
     * Netty. 异步/同步, 转换的上下文设置
     * 
     * @param client
     */
    void setClient(Client client);

    boolean isConnected();

    boolean isHavingNegotiation();

    void setHavingNegotiation(boolean havingNegotiation);

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

    @Override
    void close();
}
