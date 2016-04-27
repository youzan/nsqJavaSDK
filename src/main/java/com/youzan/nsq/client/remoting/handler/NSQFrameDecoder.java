package com.youzan.nsq.client.remoting.handler;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.enums.ResponseType;
import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.frames.ErrorFrame;
import com.youzan.nsq.client.frames.FrameType;
import com.youzan.nsq.client.frames.MessageFrame;
import com.youzan.nsq.client.frames.ResponseFrame;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NSQFrameDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(NSQFrameDecoder.class);

    public NSQFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment,
            int initialBytesToStrip) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf byteBuf) throws NSQException {

        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, byteBuf);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();
            int size = byteBuffer.limit();

            int frameType = byteBuffer.getInt();

            FrameType type = FrameType.fromCode(frameType);
            log.debug("Frame type is " + frameType + " " + type + " size " + size + " readablebytes " + (size + 4));

            if (type == null) {
                throw new NSQException("Unknown frame type:" + frameType);
            }

            switch (type) {
                case ERROR:
                    return handleError(byteBuffer, size);
                case RESPONSE:
                    return handleResponse(ctx, byteBuffer, size);
                case MESSAGE:
                    return handleMessage(byteBuffer, size);
            }

            return null;
        } catch (Exception e) {
            log.error("NSQ message decode error:", e);
            return new NSQException("message decode error", e);
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
    }

    private MessageFrame handleMessage(ByteBuffer byteBuffer, int size) {
        long ts = byteBuffer.getLong();
        int attempts = byteBuffer.getShort();
        byte[] msgId = new byte[16];
        byteBuffer.get(msgId);
        byte[] body = new byte[size - 4 - NSQMessage.MIN_SIZE_BYTES];
        byteBuffer.get(body);
        NSQMessage msg = new NSQMessage(ts, attempts, msgId, body);
        MessageFrame frame = new MessageFrame(msg);
        log.debug("decoded message with id " + new String(msgId));
        return frame;
    }

    private ErrorFrame handleError(ByteBuffer byteBuffer, int size) {
        return new ErrorFrame(readString(byteBuffer, size));
    }

    private ResponseFrame handleResponse(ChannelHandlerContext ctx, ByteBuffer byteBuffer, int size) {
        String resp = readString(byteBuffer, size);
        ResponseType type = ResponseType.fromCode(resp);
        if (type != null) {
            return new ResponseFrame(type);
        }
        return new ResponseFrame(resp);
    }

    private String readString(ByteBuffer byteBuffer, int size) {
        byte[] bodyData = new byte[size - 4];
        byteBuffer.get(bodyData);
        String resp = new String(bodyData);
        return resp;
    }
}
