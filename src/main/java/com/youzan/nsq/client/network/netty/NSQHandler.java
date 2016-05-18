package com.youzan.nsq.client.network.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.Connection;
import com.youzan.nsq.client.core.ConsumerWorker;
import com.youzan.nsq.client.network.frame.NSQFrame;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {

    private static final Logger logger = LoggerFactory.getLogger(NSQHandler.class);

    /**
     * 
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        final Connection conn = ctx.channel().attr(Connection.STATE).get();
        if (null != conn) {
            logger.info("Channel will be disconnected : {} .", conn);
        } else {
            logger.error("No connection set for {}", ctx.channel());
        }
    }

    /**
     * 
     * @param ctx
     * @param cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.channel().close();
        final Connection conn = ctx.channel().attr(Connection.STATE).get();
        if (null != conn) {
            conn.close();
        } else {
            logger.error("No connection set for {}", ctx.channel());
        }
    }

    /**
     * 
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) {
        final Connection conn = ctx.channel().attr(Connection.STATE).get();
        final ConsumerWorker worker = ctx.channel().attr(ConsumerWorker.STATE).get();
        if (null != conn && null != worker) {
            ctx.channel().eventLoop().execute(() -> worker.incoming(msg, conn));
        } else {
            if (null == conn) {
                logger.error("No connection set for {}", ctx.channel());
            }
            if (null == worker) {
                logger.error("No ConsumerWorker set for {}", ctx.channel());
            }
        }
    }

}
