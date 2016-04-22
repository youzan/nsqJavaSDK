package com.youzan.nsq.client.exceptions;

/**
 * @author caohaihong
 * since 2015年8月5日 下午5:15:47
 */
public class NSQException extends Exception {
    private static final long serialVersionUID = 9035255049714054051L;

    public NSQException(String message) {
        super(message);
    }
    
    public NSQException(String message, Throwable cause) {
        super(message, cause);
    }
}
