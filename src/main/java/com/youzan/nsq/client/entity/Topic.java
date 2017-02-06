package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Topic class with partition id
 * Created by lin on 16/8/18.
 */
public class Topic implements Comparable<Topic> {
    private static final Logger logger = LoggerFactory.getLogger(Topic.class);
    public static final Topic TOPIC_DEFAULT = new Topic("*");
    private volatile SortedMap<Address, SortedSet<Integer>> nsqdAddr2Partition;
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

    public static Topic newInstacne(final Topic topic) {
        Topic copy = new Topic(topic.getTopicText());
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
            toString = String.format("topic: %s, %d.", this.topic, this.partitionID);
        return toString;
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
     * @param seed
     * @param partitionNum
     * @return
     */
    public int updatePartitionIndex(Object seed, int partitionNum) {
        if (partitionNum <= 0) {
            //for partition Num < 0, treat it as sharding is no needed here
            return -1;
        }
        //update partitionID
        this.partitionID = this.sharding.toPartitionID(seed, partitionNum);
        return this.partitionID;
    }

    //form a address 2 partition list mapping, out of partititon 2 address mapping
    public void updateNSQdAddr2Partition(String key, final SortedMap<Address, SortedSet<Integer>> map) {
        if(!this.key.equals(key)) {
            synchronized (this.key) {
                if(!this.key.equals(key)) {
                    logger.info("NSQd addr 2 partition mapping for topic {} changed to {}.", this.getTopicText(), key);
                    this.key = key;
                    this.nsqdAddr2Partition = map;
                }
            }
        }
    }

    public SortedMap<Address, SortedSet<Integer>> getNsqdAddr2Partition() {
        return this.nsqdAddr2Partition;
    }

}
