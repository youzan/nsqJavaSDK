package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NSQFeatureDetectionHandler extends SimpleChannelInboundHandler<NSQFrame> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
    }
}
