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
     * @param string
     * @param cause
     */
    public NoConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
