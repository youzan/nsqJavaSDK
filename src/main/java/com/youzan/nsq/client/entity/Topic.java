package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.core.command.PartitionEnable;

/**
 * Topic class with partition id
 * Created by lin on 16/8/18.
 */
public class Topic implements Comparable<Topic>{
    private final String topic;
    private final int partitionId;
    private final int prime = 31;
    private String toString = null;

    /**
     * constructor to create a topic object
     * @param topic topic to scribe/publish to
     * @param partitionId partition id topic has(or may not)
     */
    public Topic(String topic, int partitionId){
        this.topic = topic;
        this.partitionId = partitionId;
    }

    Topic(){
        topic = "INVALID_TOPIC_NAME";
        partitionId = PartitionEnable.PARTITION_ID_NO_SPECIFY;
    }

    /**
     * constructor to create a topic object
     * @param topic topic to scribe/publish to
     */
    public Topic(String topic){
        this.topic = topic;
        this.partitionId = PartitionEnable.PARTITION_ID_NO_SPECIFY;
    }

    public boolean hasPartition(){
        return this.partitionId > PartitionEnable.PARTITION_ID_NO_SPECIFY;
    }

    public String getTopicText(){
        return this.topic;
    }

    public int getPartitionId(){
        return this.partitionId;
    }


    @Override
    public int hashCode() {
        return this.topic.hashCode() * prime + partitionId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Topic other = (Topic) obj;
        if(null == this.topic){
            if(null != other.topic){
                return false;
            }
        }
        return this.topic == other.topic && this.partitionId == other.partitionId;
    }

    public int compareTo(Topic other){
        return this.topic.compareTo(other.topic);
    }

    public String toString(){
        if(null == toString)
            toString = String.format("topic: %s, partition id: %s", this.topic, this.partitionId);
        return toString;
    }
}
