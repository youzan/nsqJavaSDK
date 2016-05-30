package com.youzan.nsq.client.network.netty;

import java.util.List;

import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class NSQDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        final int size = in.readInt();
        final int frameType = in.readInt();
        NSQFrame frame = NSQFrame.newInstance(frameType);
        if (frame == null) {
            // uhh, bad response from server.. what should we do?
            final String tip = String.format("Bad frame id from server (%d). It will be disconnected!", frameType);
            throw new NSQException(tip);
        }
        frame.setSize(size);
        final ByteBuf bytes = in.readBytes(size - 4);
        frame.setData(bytes.array());
        out.add(frame);
    }
}
