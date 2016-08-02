/**
 * 
 */
package com.youzan.nsq.client.exception;

public class NSQNoConnectionException extends NSQException {

    private static final long serialVersionUID = -3344028957138292433L;

    public NSQNoConnectionException(Throwable cause) {
        super("It can not connect the remote address! Please check it (maybe the network is wrong)!", cause);
    }

    public NSQNoConnectionException(String message) {
        super(message);
    }

    public NSQNoConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
