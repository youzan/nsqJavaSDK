package com.youzan.nsq.client.frames;

import com.youzan.nsq.client.remoting.handler.NSQMessage;

/**
 * @author caohaihong since 2015年10月29日 上午11:46:44
 */
public class MessageFrame implements NSQFrame {
    private final NSQMessage message;

    public MessageFrame(NSQMessage message) {
        this.message = message;
    }

    public NSQMessage getNSQMessage() {
        return message;
    }

    @Override
    public String getMessage() {
        return null;
    }
}
