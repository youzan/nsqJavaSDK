package com.youzan.nsq.client.exception;

/**
 * Created by lin on 17/3/14.
 */
public class NSQProducerNotFoundException extends NSQException {
    public NSQProducerNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public NSQProducerNotFoundException(String message) {
        super(message);
    }
}
