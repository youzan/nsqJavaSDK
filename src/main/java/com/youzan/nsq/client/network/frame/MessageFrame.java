package com.youzan.nsq.client.network.frame;

public class MessageFrame extends NSQFrame {

    @Override
    public FrameType getType() {
        return FrameType.MESSAGE_FRAME;
    }

    @Override
    public String getMessage() {
        return null;
    }
}
