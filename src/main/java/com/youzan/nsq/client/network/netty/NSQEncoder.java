package com.youzan.nsq.client.network.netty;

import java.util.List;

import com.youzan.nsq.client.core.command.NSQCommand;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class NSQEncoder extends MessageToMessageEncoder<NSQCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NSQCommand command, List<Object> out) throws Exception {
        if (command == null) {
            throw new NullPointerException("I can not encode Null-Pointer!");
        }

        if (command.getBytes() != null) {
            final byte[] bs = command.getBytes();
            if (bs.length > 0) {
                final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(bs.length);
                buf.writeBytes(bs);
                out.add(buf);
            } else {
                throw new IllegalStateException("NSQCommand isn't a right implementation!");
            }
            return;
        }

        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(command.getHeader().getBytes(NSQCommand.DEFAULT_CHARSET_NAME));

        final List<byte[]> body = command.getBody();
        assert body != null;
        // for MPUB messages.
        if (body.size() > 1) {
            // write total bodysize and message size
            int bodySize = 4; // 4 for total messages int.
            for (byte[] data : body) {
                bodySize += 4; // message size
                bodySize += data.length;
            }
            buf.writeInt(bodySize);
            buf.writeInt(body.size());
        }

        for (byte[] data : body) {
            buf.writeInt(data.length);
            buf.writeBytes(data);
        }
        out.add(buf);
        return;
    }
}
