package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.Attribute;

import java.util.List;

public class NSQDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        final int size = in.readInt();
        final int frameType = in.readInt();
        Attribute<Boolean> att =  ctx.channel().attr(Client.ORDERED);
        Boolean isOrdered = null == att.get() ? false : att.get();
        final NSQFrame frame = NSQFrame.newInstance(frameType, isOrdered);
        if (frame == null) {
            // uhh, bad response from server.. what should we do?
            final String tip = String.format("Bad frame id from server (%d). It will be disconnected!", frameType);
            throw new NSQException(tip);
        }
        frame.setSize(size);
        final byte[] body = new byte[size - 4];
        in.readBytes(body);
        frame.setData(body);
        out.add(frame);
    }
}
