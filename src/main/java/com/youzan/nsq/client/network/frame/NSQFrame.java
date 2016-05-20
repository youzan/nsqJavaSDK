package com.youzan.nsq.client.network.frame;

public abstract class NSQFrame {

    public static final String ASCII = "US-ASCII";
    public static final String UTF = "UTF-8";
    public static final String DEFAULT_CHARSET_NAME = UTF;

    public enum FrameType {
        RESPONSE_FRAME(0),//
        ERROR_FRAME(1),//
        MESSAGE_FRAME(2), //
        ;

        private int type;

        FrameType(int type) {
            this.type = type;
        }
    }

    private int size;
    private byte[] data;

    public abstract FrameType getType();

    public abstract String getMessage();

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public static NSQFrame newInstance(final int type) {
        switch (type) {
            case 0:
                return new ResponseFrame();
            case 1:
                return new ErrorFrame();
            case 2:
                return new MessageFrame();
            default:
                return null;
        }
    }
}
