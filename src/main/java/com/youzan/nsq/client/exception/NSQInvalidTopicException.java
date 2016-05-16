package com.youzan.nsq.client.exception;

public class NSQInvalidTopicException extends NSQException {

    private static final long serialVersionUID = 6410416443212221161L;

    /**
     * @param cause
     */
    public NSQInvalidTopicException(Throwable cause) {
        super(cause);
    }
}
