package com.youzan.nsq.client.network.frame;

import com.youzan.nsq.client.entity.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorFrame extends NSQFrame {

    private static final Logger logger = LoggerFactory.getLogger(ErrorFrame.class);

    @Override
    public FrameType getType() {
        return FrameType.ERROR_FRAME;
    }

    @Override
    public void setData(byte[] data) {
        super.setData(data);
        logger.warn("Message is {}", getMessage());
    }

    @Override
    public String getMessage() {
        return new String(getData(), DEFAULT_CHARSET).trim();
    }

    /**
     * @return the err
     */
    public Response getError() {
        final String content = getMessage();
        if (null != content && !content.isEmpty()) {
            if (content.startsWith("E_INVALID ")) {
                return Response.E_INVALID;
            }
            if (content.startsWith("E_BAD_TOPIC")) {
                return Response.E_BAD_TOPIC;
            }
            if (content.startsWith("E_BAD_MESSAGE")) {
                return Response.E_BAD_MESSAGE;
            }
            if (content.startsWith("E_FAILED_ON_NOT_LEADER")) {
                return Response.E_FAILED_ON_NOT_LEADER;
            }
            if (content.startsWith("E_FAILED_ON_NOT_WRITABLE")) {
                return Response.E_FAILED_ON_NOT_WRITABLE;
            }
            if (content.startsWith("E_TOPIC_NOT_EXIST")) {
                return Response.E_TOPIC_NOT_EXIST;
            }
            if (content.startsWith("E_PUB_FAILED")) {
                return Response.E_PUB_FAILED;
            }
            if (content.startsWith("E_FIN_FAILED")) {
                return Response.E_FIN_FAILED;
            }
            if (content.startsWith("E_SUB_ORDER_IS_MUST")) {
               return Response.E_SUB_ORDER_IS_MUST;
            }
            if (content.startsWith("E_TAG_NOT_SUPPORT")) {
                return Response.E_TAG_NOT_SUPPORT;
            }
            if (content.startsWith("E_SUB_EXTEND_NEED")) {
                return Response.E_SUB_EXTEND_NEED;
            }
            if (content.startsWith(Response.E_BAD_TAG.getContent())) {
                return Response.E_BAD_TAG;
            }
            if (content.startsWith(Response.E_EXT_NOT_SUPPORT.getContent())) {
                return Response.E_EXT_NOT_SUPPORT;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ErrorFrame: " + this.getMessage();
    }
}
