package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.core.command.PartitionEnable;

/**
 * Created by lin on 16/8/19.
 */
public class TopicConsumer extends Topic {

    private Topic nestedTopic;

    public TopicConsumer(Topic topic) {
        super();
        nestedTopic = topic;
    }

    public boolean equals(Object obj){
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
        if(null == this.getTopicText()){
            if(null != other.getTopicText()){
                return false;
            }
        }
        //override equals here for consumer only
        int thisPar = this.getPartitionId();
        int otherPar = other.getPartitionId();
        return this.getTopicText() == other.getTopicText() &&
                ( thisPar == otherPar || (thisPar != otherPar && thisPar + otherPar > 2 * PartitionEnable.PARTITION_ID_NO_SPECIFY) );
    }

    public Topic unwrap(){
        return this.nestedTopic;
    }

    @Override
    public boolean hasPartition() {
        return nestedTopic.hasPartition();
    }

    @Override
    public String getTopicText(){
        return nestedTopic.getTopicText();
    }

    @Override
    public int getPartitionId(){
        return nestedTopic.getPartitionId();
    }
}
