package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedSet;

/**
 * Topic class with partition id, however, containers like {@link SortedSet} or {@link java.util.HashMap} is NOT aware of
 * partition information
 * Created by lin on 16/8/18.
 */
public class Topic implements Comparable<Topic> {
    private static final Logger logger = LoggerFactory.getLogger(Topic.class);
    public static final Topic TOPIC_DEFAULT = new Topic("*");
    private String key = "";

    //topic sharding
    private static final TopicSharding<Object> TOPIC_SHARDING = new TopicSharding<Object>() {
        @Override
        public int toPartitionID(Object passInSeed, int partitionNum) {
            int code = passInSeed.hashCode() % partitionNum;
            return code >=0 ? code : -code;
        }

        @Override
        public long toShardingCode(Object passInSeed) {
            return passInSeed.hashCode();
        }
    };

    private final String topic;
    private int partitionID = -1;
    private TopicSharding sharding = TOPIC_SHARDING;


    /**
     * constructor to create a topic object
     *
     * @param topic topic to scribe/publish to
     */
    public Topic(String topic) {
        this.topic = topic;
    }

    public Topic(String topic, int partitionID) {
        this.topic = topic;
        this.partitionID =  partitionID;
    }

    public static Topic newInstacne(final Topic topic, boolean copyPar) {
        Topic copy = new Topic(topic.getTopicText());
        if(copyPar)
            copy.setPartitionID(topic.getPartitionId());
        copy.setTopicSharding(topic.getTopicSharding());
        return copy;
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

    /**
     * specify partition Id under current topic to subscribe/publish.
     * @param partitionID partiiton ID
     */
    public void setPartitionId(int partitionID) {
        this.partitionID = partitionID;
    }

    /**
     * Set partition Id for {@link com.youzan.nsq.client.Consumer} to pick partition in SUB ORDER mode.
     *
     * @param partitionID partition Id to subscribe to of current topic
     */
    public void setPartitionID(int partitionID) {
        if (partitionID != this.partitionID)
            this.partitionID = partitionID;
    }

    @Override
    public int hashCode() {
        return this.topic.hashCode() * 31 + this.partitionID;
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
        return this.topic == ((Topic) obj).topic;
    }

    public int compareTo(Topic other) {
        return this.hashCode() - other.hashCode();
    }

    public String toString() {
        return String.format("topic: %s, %d.", this.topic, this.partitionID);
    }

    public Topic setTopicSharding(TopicSharding topicSharding) {
        this.sharding = topicSharding;
        return this;
    }

    public TopicSharding getTopicSharding() {
        return this.sharding;
    }

    /**
     * this function touches current topic to set/update its current partition ID.
     *
     * @param seed          partition seed
     * @param partitionNum  partition number
     * @return partitionID  generated partition ID
     */
    public int calculatePartitionIndex(Object seed, int partitionNum) {
        if (partitionNum <= 0) {
            //for partition Num < 0, treat it as sharding is no needed here
            return -1;
        }
        //update partitionID
        return this.sharding.toPartitionID(seed, partitionNum);
    }
}
