package com.youzan.nsq.client.remoting.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.remoting.NSQConnector;
import com.youzan.nsq.client.remoting.connector.ConnectorUtils;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

public class NSQChannelHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(NSQChannelHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, final Object msg) throws Exception {
        final NSQConnector connector = ctx.channel().attr(NSQConnector.CONNECTOR).get();
        if (connector != null) {
            ctx.channel().eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    connector.incoming(msg);
                }
            });
        } else {
            log.warn("no connection set for : " + ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("nsqChannelHandler.exceptionCaught:", cause);
        ctx.close();
        NSQConnector connector = ctx.channel().attr(NSQConnector.CONNECTOR).get();
        if (connector != null) {
            connector.close();
        } else {
            log.error("no connection set for : " + ctx.channel());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        NSQConnector connector = ctx.channel().attr(NSQConnector.CONNECTOR).get();
        if (connector != null) {
            log.info("channel disconnected! " + ConnectorUtils.getConnectorKey(connector));
        } else {
            log.error("no connection set for : " + ctx.channel());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.READER_IDLE) {// read timeout
                log.error("client read timeout. (default 60s)");
                ctx.disconnect();// 触发channelInactive方法
            }
        }
    }
}
