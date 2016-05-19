package com.youzan.nsq.client.network.frame;

public class MessageFrame extends NSQFrame {

    private byte[] timestamp = new byte[8];
    private byte[] attempts = new byte[2];
    private byte[] messageID = new byte[16];
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
