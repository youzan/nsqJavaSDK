/**
 * 
 */
package com.youzan.nsq.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;

/**
 * The intersection between {@code Producer} and {@code Consumer}.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQSimpleClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    private final NSQConfig config;

    public NSQSimpleClient(NSQConfig config) {
        this.config = config;
    }

    @Override
    public void incoming(final NSQFrame frame, final NSQConnection conn) {
        switch (frame.getType()) {
            case RESPONSE_FRAME: {
                final String resp = frame.getMessage();
                if (Response._HEARTBEAT_.getContent().equals(resp)) {
                    conn.command(Nop.getInstance());
                    return;
                } else {
                    conn.addResponseFrame((ResponseFrame) frame);
                }
                break;
            }
            case ERROR_FRAME: {
                // ErrorCallback ?
                conn.addErrorFrame((ErrorFrame) frame);
                break;
            }
            default: {
                logger.error("Invalid Frame Type.");
                break;
            }
        }
        return;
    }

    @Override
    public void backoff(NSQConnection conn) throws NSQException {
        conn.command(new Rdy(0));
    }
}
