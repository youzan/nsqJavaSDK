package com.youzan.nsq.client.exception;

import com.youzan.nsq.client.entity.Response;

public class NSQInvalidMessageException extends NSQException {
    private static final long serialVersionUID = 2600952717826058158L;

    public NSQInvalidMessageException() {
        super(Response.E_BAD_MESSAGE + " Pls check if message body size exceeds limitation in NSQ.");
    }

    public NSQInvalidMessageException(String message) {
        super(message);
    }

    public NSQInvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
