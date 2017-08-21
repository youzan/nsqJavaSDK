package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.NSQSimpleClient;
import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lin on 17/6/26.
 */
public class MockedNSQSimpleClient extends NSQSimpleClient {
    private final Logger logger = LoggerFactory.getLogger(MockedNSQSimpleClient.class.getName());

    public MockedNSQSimpleClient(Role role, boolean localLookupd) {
        super(role, localLookupd);
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) throws NSQException {
        if (frame == null) {
            logger.error("The fra`me is null because of SDK's bug in the {}", this.getClass().getName());
            return;
        }
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if (Response._HEARTBEAT_.getContent().equals(resp)) {
                    ChannelFuture nopFuture = conn.command(Nop.getInstance());
                    nopFuture.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if(!future.isSuccess()) {
                                logger.error("Fail to response to heartbeat from {}.", conn.getAddress());
                            }
                        }
                    });
                    return;
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                if (conn.isSubSent()) {
//                    super.incoming(frame, conn);
                } else {
                    final ErrorFrame err = (ErrorFrame) frame;
                    conn.addErrorFrame(err);
                    logger.warn("Error-Frame from {} , frame: {}", conn.getAddress(), frame);
                }
                break;
            }
            case MESSAGE_FRAME: {
                logger.warn("Receive a message frame in the mocked simple client.");
                break;
            }
            default: {
                logger.warn("Invalid frame-type from {} , frame-type: {} , frame: {}", conn.getAddress(), frame.getType(), frame);
                break;
            }
        }
    }
}
