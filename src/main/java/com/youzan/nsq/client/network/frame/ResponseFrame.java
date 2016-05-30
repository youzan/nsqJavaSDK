package com.youzan.nsq.client.network.frame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return new String(getData(), DEFAULT_CHARSET).trim();
    }

    @Override
    public String toString() {
        return "Response: " + this.getMessage();
    }

}
