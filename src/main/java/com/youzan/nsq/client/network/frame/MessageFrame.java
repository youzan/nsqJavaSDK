package com.youzan.nsq.client.network.frame;

public class MessageFrame extends NSQFrame {

    private long timestamp;
    private int attempts;
    private byte[] messageId = new byte[16];
    private byte[] messageBody;

    @Override
    public FrameType getType() {
        return FrameType.MESSAGE_FRAME;
    }

    @Override
    public String getMessage() {
        return null;
    }
}
