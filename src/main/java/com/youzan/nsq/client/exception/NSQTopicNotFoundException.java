package com.youzan.nsq.client.exception;

import com.youzan.nsq.client.entity.Topic;

/**
 * Created by lin on 17/1/12.
 */
public class NSQTopicNotFoundException extends NSQException {
    private Topic topic;

    public NSQTopicNotFoundException(String message) {
        super(message);
    }

    public NSQTopicNotFoundException(String message, Topic topic, Throwable cause) {
        super(message, cause);
        this.topic = topic;
    }

    public Topic getTopic(){
        return this.topic;
    }
}
