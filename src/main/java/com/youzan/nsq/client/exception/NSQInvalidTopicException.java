package com.youzan.nsq.client.exception;

import com.youzan.nsq.client.entity.Response;
import com.youzan.nsq.client.entity.Topic;

public class NSQInvalidTopicException extends NSQException {

    private static final long serialVersionUID = 6410416443212221161L;

    public NSQInvalidTopicException() {
        super(Response.E_BAD_TOPIC + ": SDK may subscribe/publish to a topic and partition which does not exist.\n Please contact your administrator!");
    }

    public NSQInvalidTopicException(final Topic topic) {
        super(Response.E_BAD_TOPIC + ": SDK may subscribe/publish to a topic {" + topic.toString() + "} which does not exist.\n" +
                " Please contact your administrator!");
    }

}
