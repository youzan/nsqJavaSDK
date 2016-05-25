package com.youzan.nsq.client.network.frame;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.entity.Response;

public class ErrorFrame extends NSQFrame {

    private static final Logger logger = LoggerFactory.getLogger(ErrorFrame.class);

    private Response error;

    @Override
    public FrameType getType() {
        return FrameType.ERROR_FRAME;
    }

    @Override
    public void setData(byte[] data) {
        super.setData(data);
        error = Response.valueOf(getMessage().toUpperCase().trim());
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
        return error;
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
