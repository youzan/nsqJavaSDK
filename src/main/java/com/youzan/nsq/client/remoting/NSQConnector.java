package com.youzan.nsq.client.remoting;

import java.io.Closeable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.commands.Close;
import com.youzan.nsq.client.commands.Finish;
import com.youzan.nsq.client.commands.Identify;
import com.youzan.nsq.client.commands.Magic;
import com.youzan.nsq.client.commands.NSQCommand;
import com.youzan.nsq.client.commands.Nop;
import com.youzan.nsq.client.commands.Ready;
import com.youzan.nsq.client.commands.Requeue;
import com.youzan.nsq.client.commands.Subscribe;
import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.frames.ErrorFrame;
import com.youzan.nsq.client.frames.MessageFrame;
import com.youzan.nsq.client.frames.NSQFrame;
import com.youzan.nsq.client.frames.ResponseFrame;
import com.youzan.nsq.client.remoting.connector.NSQConfig;
import com.youzan.nsq.client.remoting.handler.NSQChannelHandler;
import com.youzan.nsq.client.remoting.handler.NSQFrameDecoder;
import com.youzan.nsq.client.remoting.handler.NSQMessage;
import com.youzan.nsq.client.remoting.listener.ConnectorListener;
import com.youzan.nsq.client.remoting.listener.NSQEvent;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Created by pepper on 14/12/22.
 */
public class NSQConnector implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NSQConnector.class);
    private String host; // nsqd host
    private int port; // nsqd tcp port
    private Channel channel;
    private EventLoopGroup workerGroup;
    private ConnectorListener subListener;
    private LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<NSQCommand>(1);
    private LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<NSQFrame>(1);
    private final NSQChannelHandler handler = new NSQChannelHandler();;
    private static final int DEFAULT_WAIT = 10;
    private static final int DEFAULT_REQ_TIMEOUT = 0;
    private static final int DEFAULT_REQ_TIMES = 10;
    private int defaultRdyCount;
    private final AtomicLong rdyCount = new AtomicLong();
    public static final AttributeKey<NSQConnector> CONNECTOR = AttributeKey.valueOf("connector");

    public NSQConnector(String host, int port, ConnectorListener subListener, int rdyCount) throws NSQException {
        this.subListener = subListener;
        this.host = host;
        this.port = port;
        this.defaultRdyCount = rdyCount;
        this.rdyCount.set(defaultRdyCount);
        workerGroup = new NioEventLoopGroup();
        Bootstrap boot = new Bootstrap();
        boot.group(workerGroup);
        boot.channel(NioSocketChannel.class);
        boot.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new IdleStateHandler(60, 0, 0));
                ch.pipeline().addLast(new NSQFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                ch.pipeline().addLast(handler);
            }
        });
        boot.option(ChannelOption.SO_KEEPALIVE, true);

        final ChannelFuture future = boot.connect(this.host, this.port);
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NSQException("can't connect to server", future.cause());
        }
        log.info("NSQConnector start, address {}:{}", host, port);

        this.channel.attr(CONNECTOR).set(this);
        sendMagic();

        Identify identify = new Identify(new NSQConfig());
        try {
            NSQFrame resp = writeAndWait(identify);
            log.info("identify response:" + resp.getMessage());
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new NSQException("send indentify goes wrong.", e);
        }
    }

    public void incoming(Object msg) {
        if (msg instanceof ResponseFrame) {
            ResponseFrame resp = (ResponseFrame) msg;
            if ("_heartbeat_".equalsIgnoreCase(resp.getMessage())) {
                nop();
            } else {
                dealResponse(resp);
            }
        } else if (msg instanceof ErrorFrame) {
            log.error("get errorFrame:" + ((ErrorFrame) msg).getError());
            dealResponse((ErrorFrame) msg);
        } else if (msg instanceof MessageFrame) {
            MessageFrame msgFrame = (MessageFrame) msg;

            NSQMessage nsqMsg = msgFrame.getNSQMessage();
            byte[] data = nsqMsg.getBody();
            String message = new String(data);
            log.debug("Received message\n" + message);

            if (subListener != null) {
                try {
                    subListener.handleEvent(new NSQEvent(NSQEvent.READ, data));
                    if (rdyCount.decrementAndGet() <= 0) {
                        rdyCount.set(defaultRdyCount);
                        finishAndRdy(nsqMsg, defaultRdyCount);
                    } else {
                        finish(nsqMsg);
                    }
                } catch (Exception e) {
                    if (nsqMsg.getAttempts() < DEFAULT_REQ_TIMES) {
                        log.warn("nsq message deal fail(will requeue) at:{}", e);
                        requeue(nsqMsg);
                    } else {
                        log.warn("nsq message deal fail and requeue times gt 10, then be finished. this messageID:{}",
                                new String(nsqMsg.getMessageId()));
                        finish(nsqMsg);
                    }

                    if (rdyCount.get() <= 0) {
                        rdy(defaultRdyCount);
                    }
                }
            } else {
                finish(nsqMsg);
                log.warn("no message listener, this message has be finished.");
            }
        } else {
            log.warn("something else error:{}", msg);
        }
    }

    private void dealResponse(NSQFrame resp) {
        try {
            this.responses.offer(resp, DEFAULT_WAIT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("response offer error:{}", e);
        }
    }

    public NSQFrame writeAndWait(NSQCommand command) throws NSQException, InterruptedException {
        if (!this.requests.offer(command, DEFAULT_WAIT, TimeUnit.SECONDS)) {
            throw new NSQException("command request offer timeout");
        }

        this.responses.clear();
        ChannelFuture cft = write(command);

        if (!cft.await(DEFAULT_WAIT, TimeUnit.SECONDS)) {
            this.requests.poll();
            throw new NSQException("command writer timeout");
        }

        NSQFrame response = this.responses.poll(DEFAULT_WAIT, TimeUnit.SECONDS);
        if (response == null) {
            this.requests.poll();
            throw new NSQException("command response poll timeout");
        }

        this.requests.poll();
        return response;
    }

    private ChannelFuture write(NSQCommand command) {
        ByteBuf buf = channel.alloc().buffer().writeBytes(command.getCommandBytes());
        return channel.writeAndFlush(buf);
    }

    private void sendMagic() {
        channel.writeAndFlush(channel.alloc().buffer().writeBytes(new Magic().getCommandBytes()));
    }

    public ChannelFuture sub(String topic, String channel) {
        Subscribe sub = new Subscribe(topic, channel);
        log.debug("Subscribing to " + sub.getCommandString());

        byte[] subBytes = sub.getCommandBytes();
        return this.channel.writeAndFlush(this.channel.alloc().buffer().writeBytes(subBytes));
    }

    public ChannelFuture finish(NSQMessage msg) {
        Finish finish = new Finish(msg.getMessageId());
        ByteBuf buf = channel.alloc().buffer().writeBytes(finish.getCommandBytes());
        return channel.writeAndFlush(buf);
    }

    public ChannelFuture requeue(NSQMessage msg) {
        Requeue requeue = new Requeue(msg.getMessageId(), DEFAULT_REQ_TIMEOUT);
        ByteBuf buf = channel.alloc().buffer().writeBytes(requeue.getCommandBytes());
        return channel.writeAndFlush(buf);
    }

    public ChannelFuture rdy(int count) {
        Ready rdy = new Ready(count);
        byte[] rdyBytes = rdy.getCommandBytes();
        return channel.writeAndFlush(channel.alloc().buffer().writeBytes(rdyBytes));
    }

    public ChannelFuture finishAndRdy(NSQMessage msg, final int count) {
        return finish(msg).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                rdy(count);
            };
        });
    }

    public ChannelFuture nop() {
        return channel.writeAndFlush(channel.alloc().buffer().writeBytes(new Nop().getCommandBytes()));
    }

    private void cleanClose() {
        try {
            NSQFrame resp = writeAndWait(new Close());
            if (resp instanceof ErrorFrame) {
                log.warn("cleanClose {}:{} goes error at:{}", host, port, resp.getMessage());
            }
        } catch (NSQException | InterruptedException e) {
            log.warn("cleanClose {}:{} goes error at:{}", host, port, e);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            if (channel.isActive()) {
                cleanClose();
            }
            channel.disconnect();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public boolean isConnected() {
        return channel == null ? false : channel.isActive() ? true : false;
    }

    public int getDefaultRdyCount() {
        return defaultRdyCount;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
