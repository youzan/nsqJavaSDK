package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.IOUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {

    private static final Logger logger = LoggerFactory.getLogger(NSQHandler.class);

    /**
     * @param ctx ChannelHandlerContext
     * @throws Exception if an error occurs
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    /**
     * @param ctx   ChannelHandlerContext
     * @param cause Throwable
     * @throws Exception if an error occurs
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        destroy(ctx);
    }

    /**
     * @param ctx ChannelHandlerContext
     * @param msg a {@link NSQFrame}
     */
    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final NSQFrame msg) {
        final NSQConnection conn = ctx.channel().attr(NSQConnection.STATE).get();
        final Client worker = ctx.channel().attr(Client.STATE).get();
        if (null != conn && null != worker) {
            ctx.channel().eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        worker.incoming(msg, conn);
                    } catch (Exception e) {
                        logger.error("Exception", e);
                    }
                }
            });
        } else {
            if (null == conn) {
                logger.warn("No connection set for {}", ctx.channel());
            }
            if (null == worker) {
                logger.warn("No worker set for {}", ctx.channel());
            }
            logger.warn("The original NSQFrame: {}", msg);
            destroy(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            if (((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
                final NSQConnection conn = ctx.channel().attr(NSQConnection.STATE).get();
                final Client worker = ctx.channel().attr(Client.STATE).get();
                if (worker != null && conn != null && !worker.validateHeartbeat(conn)) {
                    destroy(ctx);
                }
            }
        }
    }

    /**
     * Do it very very quietly!
     */
    private void destroy(final ChannelHandlerContext ctx) {
        if (null == ctx) {
            return;
        }
        final NSQConnection conn = ctx.channel().attr(NSQConnection.STATE).get();
        final Client worker = ctx.channel().attr(Client.STATE).get();
        if (worker != null && conn != null) {
            worker.close(conn);
        } else {
            final Channel c = ctx.channel();
            if (c != null) {
                c.close();
                c.deregister();
            }
        }
    }
}
