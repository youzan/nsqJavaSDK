package com.youzan.nsq.client.network.frame;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.core.command.NSQCommand;

public class ResponseFrame extends NSQFrame {
    private static final Logger logger = LoggerFactory.getLogger(ResponseFrame.class);

    @Override
    public FrameType getType() {
        return FrameType.RESPONSE_FRAME;
    }

    /**
     * @return
     */
    @Override
    public String getMessage() {
        try {
            return new String(super.getData(), NSQCommand.UTF8);
        } catch (UnsupportedEncodingException e) {
            logger.error("Exception", e);
        }
        return null;
    }

    @Override
    public String toString() {
        return "Response: " + this.getMessage();
    }

}
