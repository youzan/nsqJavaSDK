package com.youzan.nsq.client.exception;


/**
 * Created by lin on 17/1/12.
 */
public class NSQTopicNotFoundException extends NSQException {
    private String topic;

    public NSQTopicNotFoundException(String message) {
        super(message);
    }

    public NSQTopicNotFoundException(String message, String topic, Throwable cause) {
        super(message, cause);
        this.topic = topic;
    }

    public String getTopic(){
        return this.topic;
    }
}
