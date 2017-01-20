package com.youzan.nsq.client.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * NSQ message class. Instance of Message presents one NSQ message will be sent from producer.
 * Created by lin on 16/10/28.
 */
public class Message {
    private final static Logger logger = LoggerFactory.getLogger(Message.class);
    private final static String MSG_FORMAT = "Message:\n[TraceID: %d, TopicSharding: %d, Topic: %s]";
    public final static Object NO_SHARDING = new Object() {
        public int hashCode() {
            return -1;
        }
    };
    //trace id per message attached
    //meta-data need initialized in received message
    private final long traceID;
    //topic sharding ID, when larger than 0L, it is a valid sharding
    private Object topicSharding = NO_SHARDING;
    private boolean traced = false;
    private final Topic topic;

    //common part, message body
    private final String messageBody;

    public String toString() {
        return String.format(MSG_FORMAT, this.traceID, this.topicSharding, this.topic.getTopicText());
    }

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

    public Message setTopicShardingIDObject(Object shardingIDObj){
        this.topicSharding = shardingIDObj;
        return this;
    }

    /**
     * Set topic sharding with {@link Long}
     * @param shardingIDLong sharding id long type
     * @return
     */
    public Message setTopicShardingIDLong(long shardingIDLong){
        return setTopicShardingIDObject(shardingIDLong);
    }

    /**
     * Set topic shardingID with {@link String}
     * @param shardingIDString shardingId String type
     * @return
     */
    public Message setTopicShardingIDString(String shardingIDString){
        return setTopicShardingIDObject(shardingIDString);
    }

    /**
     * returns shardingId in {@link Object}
     * @return shardingId object
     */
    public Object getTopicShardingId(){
        return this.topicSharding;
    }
}
