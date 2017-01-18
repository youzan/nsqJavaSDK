package com.youzan.nsq.client.exception;

/**
 * ConfigAccessAgent Exception when we fail to initialize {@link com.youzan.nsq.client.configs.ConfigAccessAgent} in
 * current process context.
 * Created by lin on 17/1/17.
 */
public class ConfigAccessAgentInitializeException extends ConfigAccessAgentException {
    public ConfigAccessAgentInitializeException(String msg, Throwable cause){
        super(msg, cause);
    }

    public ConfigAccessAgentInitializeException(String msg){
        super(msg);
    }
}
