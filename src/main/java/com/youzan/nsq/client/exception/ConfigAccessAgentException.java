package com.youzan.nsq.client.exception;

/**
 * Created by lin on 17/1/17.
 */
public abstract class ConfigAccessAgentException extends Exception{

    public ConfigAccessAgentException(String msg, Throwable cause){
       super(msg, cause);
    }

    public ConfigAccessAgentException(String msg) {
        super(msg);
    }
}
