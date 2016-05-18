package com.youzan.nsq.client.network.netty;

import java.util.List;

import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public class NSQDecoder extends MessageToMessageDecoder<ByteBuf> {

    private int size;
    private NSQFrame frame;

    public NSQDecoder() {
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        size = in.readInt();
        final int frameType = in.readInt();
        frame = NSQFrame.newInstance(frameType);
        if (frame == null) {
            // uhh, bad response from server.. what should we do?
            // JDK8 : string
            throw new NSQException("Bad frame id from server (" + frameType + ").  disconnect!");
        }
        frame.setSize(size);
        final ByteBuf bytes = in.readBytes(size - 4);
        frame.setData(bytes.array());
        out.add(frame);
    }
}
