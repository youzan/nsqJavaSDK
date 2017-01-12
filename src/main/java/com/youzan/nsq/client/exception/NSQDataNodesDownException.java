/**
 *
 */
package com.youzan.nsq.client.exception;

public class NSQDataNodesDownException extends NSQException {
    private static final long serialVersionUID = 2336078064990893992L;

    public NSQDataNodesDownException() {
        super("SDK has done its best effort! All of the data nodes(server-side) are down! Please check both the client and the server!");
    }

    public NSQDataNodesDownException(Throwable cause) {
        super("SDK has done its best effort! All of the data nodes(server-side) are down! Please check both the client and the server!", cause);
    }

    public NSQDataNodesDownException(String message, Throwable cause) {
        super(message, cause);
    }

    public NSQDataNodesDownException(String message) {
        super(message);
    }
}
