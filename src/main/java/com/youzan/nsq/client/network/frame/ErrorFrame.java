package com.youzan.nsq.client.network.frame;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorFrame extends NSQFrame {

    private static final Logger logger = LoggerFactory.getLogger(ErrorFrame.class);

    @Override
    public FrameType getType() {
        return FrameType.ERROR_FRAME;
    }

    @Override
    public String getMessage() {
        try {
            return new String(getData(), DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            logger.error("Exception", e);
            return new String(getData());
        }
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
