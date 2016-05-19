package com.youzan.nsq.client.network.frame;

public class ErrorFrame extends NSQFrame {

    @Override
    public FrameType getType() {
        return FrameType.ERROR_FRAME;
    }

    @Override
    public String getMessage() {
        return null;
    }
}
