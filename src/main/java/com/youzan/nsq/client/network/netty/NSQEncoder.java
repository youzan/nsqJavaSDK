package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.core.command.NSQCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class NSQEncoder extends MessageToMessageEncoder<NSQCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NSQCommand command, List<Object> out) throws Exception {
        if (command == null) {
            throw new NullPointerException("I can not encode Null-Pointer!");
        }

        final byte[] bs = command.getBytes();
        if(null == bs)
            throw new IllegalStateException("Command bytes is null, current command need to impl getBytes interface. Command: " + command.toString());
        if (bs.length > 0) {
            final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(bs.length);
            buf.writeBytes(bs);
            out.add(buf);
        } else {
            throw new IllegalStateException("NSQCommand isn't a right implementation!");
        }
        return;
    }
}
