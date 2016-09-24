package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;

/**
 * Created by lin on 16/9/9.
 */
public class PubOrdered extends PubTrace {

    public PubOrdered(Topic topic, byte[] data) {
        super(topic, data);
    }

    @Override
    public String getHeader() {
        return String.format("PUB_ORDERED %s%s\n", topic.getTopicText(), topic.hasPartition() ? SPACE_STR + topic.getPartitionId() : "");
    }
}
