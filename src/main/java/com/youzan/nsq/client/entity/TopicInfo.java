package com.youzan.nsq.client.entity;

import java.io.Serializable;

/**
 * Created by lin on 17/12/12.
 */
public class TopicInfo implements Serializable {
    private String topicName;
    private int partition;
    private boolean isExt;

    public TopicInfo() {

    }

    public TopicInfo(String topicName, int partition, boolean isExt) {
        this.topicName = topicName;
        this.partition = partition;
        this.isExt = isExt;
    }

    public String getTopicName() {
        return this.topicName;
    }

    public int getTopicPartition() {
        return this.partition;
    }

    public boolean isExt() {
        return this.isExt;
    }

    public String toString() {
        return this.topicName + ", " + this.partition + ", " + this.isExt;
    }
}