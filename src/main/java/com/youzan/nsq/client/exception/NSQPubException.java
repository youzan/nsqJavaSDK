package com.youzan.nsq.client.exception;

import org.slf4j.Logger;

import java.util.List;

/**
 * Created by lin on 17/1/11.
 */
public class NSQPubException extends NSQException {

    private List<NSQException> exptions;

    public NSQPubException(String message, final List<NSQException> exptions) {
        super(message);
        this.exptions = exptions;
    }

    public NSQPubException(final List<NSQException> exptions) {
        super("Fail to pub message. Refer to nested exceptions for details.");
        this.exptions = exptions;
    }

    public void punchExceptions(Logger logger) {
        int idx = 0;
        for(Exception exp : this.exptions){
            logger.error("Nested exception {}:", ++idx, exp);
        }
    }

    public List<? extends NSQException> getNestedExceptions(){
        return this.exptions;
    }
}
