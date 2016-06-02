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
public class NSQDataNodesDownException extends NSQException {
    private static final long serialVersionUID = 2336078064990893992L;

    private static final Logger logger = LoggerFactory.getLogger(NSQDataNodesDownException.class);

    /**
     * 
     */
    public NSQDataNodesDownException() {
        super("SDK has done its best effort! All of the data node servers are donw! Please check both the client and the server!");
    }
}
