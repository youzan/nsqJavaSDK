package com.youzan.nsq.client.exception;

/**
 * Exception thrown in message handler to explicitly indicate SDK that message need requeue.
 * When SDK catches it, it requeues current message, without print error log.
 * Created by lin on 17/10/17.
 */
public class ExplicitRequeueException extends RuntimeException {
    private final boolean depressWarnLog;

    public ExplicitRequeueException(String message) {
        super(message);
        this.depressWarnLog = true;
    }

    public ExplicitRequeueException(String message, boolean depressWarnLog) {
        super(message);
        this.depressWarnLog = depressWarnLog;
    }

    public boolean isWarnLogDepressed() {
        return this.depressWarnLog;
    }
}
