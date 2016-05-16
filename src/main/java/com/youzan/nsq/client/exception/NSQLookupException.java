/**
 * 
 */
package com.youzan.nsq.client.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQLookupException extends NSQException {

    private static final long serialVersionUID = -7520428599551287228L;
    private static final Logger logger = LoggerFactory.getLogger(NSQLookupException.class);

    /**
     * @param cause
     */
    public NSQLookupException(Throwable cause) {
        super(cause);
    }

    /**
     * @param string
     * @param e
     */
    public NSQLookupException(String message, Throwable cause) {
        super(message, cause);
    }
}
