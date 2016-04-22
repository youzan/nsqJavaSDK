package com.youzan.nsq.client.frames;

/**
 * @author caohaihong since 2015年10月29日 下午2:51:04
 */
public class ErrorFrame implements NSQFrame {
    private String error;

    public ErrorFrame(String error) {
        this.error = error;
    }

    public String getError() {
        return error;
    }

    @Override
    public String getMessage() {
        return error;
    }
}
