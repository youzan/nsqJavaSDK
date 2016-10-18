package com.youzan.nsq.client.entity;

/**
 * customized topic object for trace switch check
 * Created by lin on 16/9/21.
 */
public class TopicTrace extends Topic{

    public TopicTrace(String topic) {
        super(topic);
    }

    public int hashCode(){
        return super.getTopicText().hashCode();
    }

    @Override
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
       TopicTrace other = (TopicTrace) obj;
       if(null == this.getTopicText()){
           if(null != other.getTopicText()){
               return false;
           }
       }
       return this.getTopicText().equals(other.getTopicText());
    }
}
