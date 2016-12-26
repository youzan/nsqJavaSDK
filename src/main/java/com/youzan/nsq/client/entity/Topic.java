package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic class with partition id
 * Created by lin on 16/8/18.
 */
public class Topic implements Comparable<Topic> {
    private static final Logger logger = LoggerFactory.getLogger(Topic.class);
    public static final Topic TOPIC_DEFAULT = new Topic("*");

    //topic sharding
    private static final TopicSharding<Long> TOPIC_SHARDING = new TopicSharding<Long>() {
        @Override
        public int toPartitionID(Long passInSeed, int partitionNum) {
            if (passInSeed < 0L)
                return -1;
            return (int) (passInSeed % partitionNum);
        }
    };

    private final String topic;
    private int partitionID = -1;
    private String toString = null;
    private TopicSharding sharding = TOPIC_SHARDING;


    /**
     * constructor to create a topic object
     *
     * @param topic topic to scribe/publish to
     */
    public Topic(String topic) {
        this.topic = topic;
    }

    public String getTopicText() {
        return this.topic;
    }

    public boolean hasPartition() {
        return this.partitionID >= 0;
    }

    public int getPartitionId() {
        return this.partitionID;
    }


    public void setToString(String toString) {
        this.toString = toString;
    }

    /**
     * Set partition Id for {@link com.youzan.nsq.client.Consumer} to pick partition in SUB ORDER mode.
     *
     * @param partitionID partition Id to subscribe to of current topic
     */
    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    @Override
    public int hashCode() {
        return this.topic.hashCode();
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
        if (null == this.topic) {
            if (null != other.topic) {
                return false;
            }
        }
        return this.topic.equals(other.getTopicText());
    }

    public int compareTo(Topic other) {
        return this.hashCode() - other.hashCode();
    }

    public String toString() {
        if (null == toString)
            toString = String.format("topic: %s.", this.topic);
        return toString;
    }

    public Topic setTopicSharding(TopicSharding topicSharding) {
        this.sharding = topicSharding;
        return this;
    }

    /**
     * this function touches current topic to set/update its current partition ID.
     *
     * @param seed
     * @param partitionNum
     * @return
     */
    public int updatePartitionIndex(long seed, int partitionNum) {
        if (partitionNum <= 0) {
            //for partition Num < 0, treat it as sharding is no needed here
            return -1;
        }
        //update partitionID
        this.partitionID = this.sharding.toPartitionID(seed, partitionNum);
        return this.partitionID;

    }
}
