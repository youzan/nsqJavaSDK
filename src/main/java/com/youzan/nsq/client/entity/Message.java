package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * NSQ message class. Instance of Message presents one NSQ message will be sent from producer.
 * Created by lin on 16/10/28.
 */
public class Message implements MessageMetadata {
    private final static Logger logger = LoggerFactory.getLogger(Message.class);

    //trace id per message attached
    //meta-data need initialized in received message
    private final long traceID;
    //topic sharding ID, when larger than 0L, it is a valid sharding
    private long topicSharding = -1L;
    private boolean traced = false;
    private final Topic topic;

    //common part, message body
    private final String messageBody;
    private String metadataStr;

    public static Message create(Topic topic, long traceID, String messageBody){
        return new Message(traceID, topic, messageBody);
    }

    public static Message create(Topic topic, String messageBody){
        return new Message(topic, messageBody);
    }

    Message(long traceID, Topic topic, String messageBody) {
        this.traceID = traceID;
        this.topic = topic;
        this.messageBody = messageBody;
    }

    Message(Topic topic, String messageBody){
        this(0L, topic, messageBody);
    }

    public long getTraceID(){
        return this.traceID;
    }

    public String getMessageBody(){
        return this.messageBody;
    }

    public byte[] getMessageBodyInByte(){
        return this.messageBody.getBytes(Charset.defaultCharset());
    }

    public Topic getTopic(){
        if(null != this.topic)
        return this.topic;
        else{
            logger.warn("Topic for message is not specified.");
            return null;
        }
    }

    public Message traced(){
        this.traced = true;
        return this;
    }

    public boolean isTraced(){
        return this.traced;
    }

    public Message setTopicShardingID(long shardingID){
        this.topicSharding = shardingID;
        return this;
    }

    public long getTopicShardingId(){
        return this.topicSharding;
    }

    @Override
    public String toMetadataStr() {
        if(null == this.metadataStr) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.toString() + " meta-data:\n");
            sb.append("\t[traceID]:\t").append(this.getTraceID());
            sb.append("\t[topic]:\t").append(this.getTopic().getTopicText());
            sb.append(this.toString() + " end.");
            this.metadataStr = sb.toString();
        }
        return this.metadataStr;
    }
}
