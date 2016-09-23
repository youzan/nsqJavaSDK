package com.youzan.nsq.client.entity;

/**
 * Created by lin on 16/9/21.
 */
public class TopicTraceAgent extends Topic{
    private Topic nestedTopic;

    public TopicTraceAgent(Topic topic) {
        super();
        nestedTopic = topic;
    }

    public int hashCode(){
        return this.nestedTopic.getTopicText().hashCode();
    }

    public Topic unwrap(){
        return this.nestedTopic;
    }
}
