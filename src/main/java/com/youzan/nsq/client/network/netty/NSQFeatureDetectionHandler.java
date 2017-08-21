package com.youzan.nsq.client.network.netty;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

public class NSQFeatureDetectionHandler extends SimpleChannelInboundHandler<NSQFrame> {

    private static final Logger logger = LoggerFactory.getLogger(ErrorFrame.class);

    private boolean ssl;
    private boolean compression;
    private boolean snappy;
    private boolean deflate;
    private int deflatLevel = 0;
    private boolean finished;

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final NSQFrame msg) throws Exception {
        final NSQConnection con = ctx.channel().attr(NSQConnection.STATE).get();
        final NSQConfig config = con.getConfig();
        boolean reinstallDefaultDecoder = true;
        if (msg instanceof ResponseFrame) {
            ResponseFrame response = (ResponseFrame) msg;
            if (logger.isDebugEnabled())
                logger.debug("Channel has received a frame, that is  {}", msg);
            ChannelPipeline pipeline = ctx.channel().pipeline();
            parseIdentify(response.getMessage(), config);
            if (response.getMessage().equals("OK")) {
                if (finished) {
                    return;
                }
                // round 2
                if (snappy) {
                    reinstallDefaultDecoder = installSnappyDecoder(pipeline);
                }
                if (deflate) {
                    reinstallDefaultDecoder = installDeflateDecoder(pipeline);
                }
                eject(reinstallDefaultDecoder, pipeline);
                if (ssl) {
                    ((SslHandler) pipeline.get("SSLHandler")).setSingleDecode(false);
                }
                return;
            }
            if (ssl) {
                SSLEngine sslEngine = config.getSslContext().newEngine(ctx.channel().alloc());
                sslEngine.setUseClientMode(true);
                SslHandler sslHandler = new SslHandler(sslEngine, false);
                sslHandler.setSingleDecode(true);
                pipeline.addBefore("LengthFieldBasedFrameDecoder", "SSLHandler", sslHandler);
                if (snappy) {
                    pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyEncoder());
                }
                if (deflate) {
                    pipeline.addBefore("NSQEncoder", "DeflateEncoder",
                            ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE, deflatLevel));
                }
            }
            if (!ssl && snappy) {
                pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyEncoder());
                reinstallDefaultDecoder = installSnappyDecoder(pipeline);
            }
            if (!ssl && deflate) {
                pipeline.addBefore("NSQEncoder", "DeflateEncoder",
                        ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE, deflatLevel));
                reinstallDefaultDecoder = installDeflateDecoder(pipeline);
            }
            if (response.getMessage().contains("version") && finished) {
                eject(reinstallDefaultDecoder, pipeline);
            }
        } // see @{code ResponseFrame}
        ctx.fireChannelRead(msg);
    }

    private void eject(final boolean reinstallDefaultDecoder, final ChannelPipeline pipeline) {
        // ok we read only the the first message to set up the pipline, ejecting
        // now!
        pipeline.remove(this);
        if (reinstallDefaultDecoder) {
            final int Integer_BYTES = 4;
            pipeline.replace("LengthFieldBasedFrameDecoder", "LengthFieldBasedFrameDecoder",
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer_BYTES));
        }
    }

    private boolean installDeflateDecoder(final ChannelPipeline pipeline) {
        finished = true;
        pipeline.replace("LengthFieldBasedFrameDecoder", "DeflateDecoder",
                ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE));
        return false;
    }

    private boolean installSnappyDecoder(final ChannelPipeline pipeline) {
        finished = true;
        pipeline.replace("LengthFieldBasedFrameDecoder", "SnappyDecoder", new SnappyFrameDecoder());
        return false;
    }

    private void parseIdentify(final String message, final NSQConfig config) {
        if ("OK".equals(message)) {
            return;
        }
        if (message.contains("\"tls_v1\":true")) {
            ssl = true;
        }
        if (message.contains("\"snappy\":true")) {
            snappy = true;
            compression = true;
        }
        if (message.contains("\"deflate\":true")) {
            deflate = true;
            compression = true;
            deflatLevel = Integer.valueOf(message.split("\"deflate_level\":",2)[1].split(",", 2)[0]);
            logger.info("deflate level: {}", deflatLevel);
        }
        if (!ssl && !compression) {
            finished = true;
        }
    }
}
