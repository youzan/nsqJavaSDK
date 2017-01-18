package com.youzan.nsq.client.exception;

/**
 * Exception when {@link com.youzan.nsq.client.PubCmdFactory} fail to initialize.
 * Created by lin on 17/1/18.
 */
public class NSQPubFactoryInitializeException extends NSQException {
    public NSQPubFactoryInitializeException(String message) {
        super(message);
    }

    public NSQPubFactoryInitializeException(String message, Throwable cause){
        super(message, cause);
    }
}
