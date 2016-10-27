/**
 *
 */
package com.youzan.nsq.client.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSQInvalidDataNodeException extends NSQException {

    private static final Logger logger = LoggerFactory.getLogger(NSQInvalidDataNodeException.class);
    private static final long serialVersionUID = -6340688420348997379L;

    public NSQInvalidDataNodeException(String topic) {
        super("Please pick up another broker! The topic is " + topic);
    }
}
