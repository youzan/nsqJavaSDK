/**
 * 
 */
package com.youzan.nsq.client.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSQLookupException extends NSQException {

    private static final long serialVersionUID = -7520428599551287228L;
    private static final Logger logger = LoggerFactory.getLogger(NSQLookupException.class);

    public NSQLookupException(String message) {
        super(message);
    }

    public NSQLookupException(Throwable cause) {
        super(cause);
    }

    public NSQLookupException(String message, Throwable cause) {
        super(message, cause);
    }

}
