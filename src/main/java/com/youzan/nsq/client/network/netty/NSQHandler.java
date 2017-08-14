package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.util.HostUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

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
        //stop propagation
        logger.warn("Uncaught exception in connection pipeline", cause);
        //add cause into error frame
        destroy(ctx);
        ctx.close();
    }

    /**
     * @param ctx ChannelHandlerContext
     * @param msg a {@link NSQFrame}
     */
    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final NSQFrame msg) {

        if(logger.isDebugEnabled() && msg.getType() == NSQFrame.FrameType.MESSAGE_FRAME) {
            MessageFrame msgFrame = (MessageFrame) msg;
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            InetAddress inetaddress = socketAddress.getAddress();
            String ipAddress = inetaddress.getHostAddress();
            logger.debug("Message frame received from: {}. Message: {}.", ipAddress, msgFrame.toString());
            try {
                logger.debug("Message to {}.", HostUtil.getLocalIP());
            } catch (IOException e) {
                logger.warn("Could not fetch local IP address for debug trace. Ignore.");
            }
        }
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
            ctx.close();
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
        //do not close connection if client is producer, as it will be closed AFTER error frame returned
        if (worker instanceof Producer) {
            logger.info("Closing worker as producer.");
            return;
        }
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
