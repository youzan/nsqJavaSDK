package com.youzan.nsq.client.exception;

import com.youzan.nsq.client.entity.Response;

public class NSQInvalidTopicException extends NSQException {

    private static final long serialVersionUID = 6410416443212221161L;

    public NSQInvalidTopicException(final String topic) {
        super(Response.E_BAD_TOPIC + " ! Please contact your administrator! The topic is " + topic);
    }

}
