package com.youzan.nsq.client.network.netty;

import java.util.List;

import com.youzan.nsq.client.core.command.NSQCommand;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class NSQEncoder extends MessageToMessageEncoder<NSQCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NSQCommand msg, List<Object> out) throws Exception {
    }
}
