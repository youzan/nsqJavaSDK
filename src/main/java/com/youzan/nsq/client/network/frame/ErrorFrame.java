package com.youzan.nsq.client.network.frame;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.entity.Response;

public class ErrorFrame extends NSQFrame {

    private static final Logger logger = LoggerFactory.getLogger(ErrorFrame.class);

    @Override
    public FrameType getType() {
        return FrameType.ERROR_FRAME;
    }

    @Override
    public void setData(byte[] data) {
        super.setData(data);
        logger.debug("Message is {}", getMessage());
    }

    @Override
    public String getMessage() {
        try {
            return new String(getData(), DEFAULT_CHARSET_NAME).trim();
        } catch (UnsupportedEncodingException e) {
            logger.error("Exception", e);
            return new String(getData()).trim();
        }
    }

    /**
     * @return the err
     */
    public Response getError() {
        final String content = getMessage();
        if (null != content) {
            if (content.startsWith("E_INVALID ")) {
                return Response.E_INVALID;
            }
            if (content.startsWith("E_BAD_TOPIC")) {
                return Response.E_BAD_TOPIC;
            }
            if (content.startsWith("E_BAD_MESSAGE")) {
                return Response.E_BAD_MESSAGE;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
