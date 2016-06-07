/**
 * 
 */
package com.youzan.nsq.client.exception;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NoConnectionException extends NSQException {

    private static final long serialVersionUID = -3344028957138292433L;

    /**
     * @param cause
     */
    public NoConnectionException(Throwable cause) {
        super("It can not the remote address! Please check it (maybe the network is wrong)!", cause);
    }

    /**
     * @param message
     */
    public NoConnectionException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public NoConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
