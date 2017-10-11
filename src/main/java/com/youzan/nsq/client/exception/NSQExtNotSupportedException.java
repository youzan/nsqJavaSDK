package com.youzan.nsq.client.exception;

/**
 * Created by lin on 17/9/30.
 */
public class NSQExtNotSupportedException extends NSQException {
    public NSQExtNotSupportedException(Throwable cause) {
        super(cause);
    }

    public NSQExtNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NSQExtNotSupportedException(String message) {
        super(message);
    }
}
