package com.youzan.nsq.client.exception;

import java.util.concurrent.TimeoutException;

/**
 * Created by lin on 17/6/19.
 */
public class NSQTimeoutException extends NSQException{
    private final TimeoutException nested;

    public NSQTimeoutException(TimeoutException te) {
        super(te.getLocalizedMessage(), te.getCause());
        this.nested = te;
    }
}
