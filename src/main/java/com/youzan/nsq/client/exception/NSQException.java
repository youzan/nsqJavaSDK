package com.youzan.nsq.client.exception;

public class NSQException extends Exception {

    private static final long serialVersionUID = 6759799779448168356L;

    public NSQException(Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    public NSQException(String message, Throwable cause) {
        super(message, cause);
    }
}
