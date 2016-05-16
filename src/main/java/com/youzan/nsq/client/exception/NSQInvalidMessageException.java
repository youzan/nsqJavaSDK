package com.youzan.nsq.client.exception;

public class NSQInvalidMessageException extends NSQException {
    private static final long serialVersionUID = 2600952717826058158L;

    /**
     * @param cause
     */
    public NSQInvalidMessageException(Throwable cause) {
        super(cause);
    }

}
