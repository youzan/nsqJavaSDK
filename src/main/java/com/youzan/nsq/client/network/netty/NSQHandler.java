package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {

    /**
     * 
     * @param ctx
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // TODO - implement NSQHandler.channelInactive
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO - implement NSQHandler.exceptionCaught
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) {
        // TODO - implement NSQHandler.channelRead0
        throw new UnsupportedOperationException();
    }

}
