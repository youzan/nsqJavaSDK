/**
 * 
 */
package com.youzan.nsq.client.exception;

public class NoConnectionException extends NSQException {

    private static final long serialVersionUID = -3344028957138292433L;

    public NoConnectionException(Throwable cause) {
        super("It can not the remote address! Please check it (maybe the network is wrong)!", cause);
    }

    public NoConnectionException(String message) {
        super(message);
    }

    public NoConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
