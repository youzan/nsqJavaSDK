package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.channel.ChannelHandlerContext;

public class NSQHandler {

    /**
     * 
     * @param ctx
     */
    public void channelInactive(ChannelHandlerContext ctx) {
        // TODO - implement NSQHandler.channelInactive
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param ctx
     * @param cause
     */
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO - implement NSQHandler.exceptionCaught
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param ctx
     * @param msg
     */
    public void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) {
        // TODO - implement NSQHandler.channelRead0
        throw new UnsupportedOperationException();
    }

}