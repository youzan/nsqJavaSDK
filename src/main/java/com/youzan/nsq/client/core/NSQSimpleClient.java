/**
 * 
 */
package com.youzan.nsq.client.core;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.NSQCommand;
import com.youzan.nsq.client.core.command.Nop;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.ErrorFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import com.youzan.nsq.client.network.frame.ResponseFrame;
import com.youzan.util.IOUtil;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQSimpleClient implements Client {
    private static final Logger logger = LoggerFactory.getLogger(NSQSimpleClient.class);

    /**
     * See {@code Producer} and {@code Consumer}
     */
    @Override
    public NSQConfig getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incoming(final NSQFrame frame, final Connection conn) {
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
    public void identify(final Connection conn, NSQConfig config) throws NSQException {
        conn.command(Magic.getInstance());
        final NSQCommand ident = new Identify(config);
        try {
            final NSQFrame response = conn.commandAndGetResponse(ident);
            if (null == response) {
                IOUtil.closeQuietly(conn);
                throw new NSQException("Bad Identify Response!");
            }
        } catch (final TimeoutException e) {
            IOUtil.closeQuietly(conn);
            throw new NSQException("Client Performance Issue", e);
        }
    }
}
