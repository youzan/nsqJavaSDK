package com.youzan.nsq.client.exception;

/**
 * Created by lin on 18/4/8.
 */
public class NSQInvalidMessageBodyException extends NSQException {
    public NSQInvalidMessageBodyException(String message) {
        super(message);
    }

    public NSQInvalidMessageBodyException() {
        super("NSQ server fail to parse invalid message body.");
    }
}
