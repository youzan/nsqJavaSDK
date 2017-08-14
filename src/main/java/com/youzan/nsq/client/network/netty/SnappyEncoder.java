package com.youzan.nsq.client.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;

/**
 * As netty builtin {@link io.netty.handler.codec.compression.SnappyFrameEncoder} does not work with nsqd, switch to
 * apcahe compress lib
 * Created by lin on 17/8/12.
 */
public class SnappyEncoder extends MessageToByteEncoder<ByteBuf> {

    private final static CompressorStreamFactory compressFactory = new CompressorStreamFactory();

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, ByteBuf cmdBuf, ByteBuf byteBuf) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedOutputStream bufos = new BufferedOutputStream(bos);
        CompressorOutputStream cos = compressFactory.createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, bufos);
        cmdBuf.readBytes(cos, cmdBuf.readableBytes());
        cos.close();
        byteBuf.writeBytes(bos.toByteArray());
    }
}

