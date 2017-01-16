package com.youzan.nsq.client.exception;

/**
 * Created by lin on 17/1/16.
 */
public class NSQPartitionNotAvailableException extends NSQException {
    public NSQPartitionNotAvailableException(String message) {
        super(message);
    }
}
