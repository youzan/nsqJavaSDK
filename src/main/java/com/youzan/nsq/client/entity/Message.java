package com.youzan.nsq.client.entity;

import com.youzan.util.IOUtil;
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
    private final byte[] messageBody;
    private Object jsonHeaderExt;
    private String desiredTag = null;

    public String toString() {
        return String.format(MSG_FORMAT, this.traceID, this.topicSharding, this.topic.getTopicText());
    }

    public static Message create(Topic topic, long traceID, String messageBody){
        return new Message(traceID, topic, messageBody.getBytes(IOUtil.DEFAULT_CHARSET));
    }

    public static Message create(Topic topic, byte[] messageBody) {
        return new Message(topic, messageBody);
    }

    public static Message create(Topic topic, String messageBody){
        return new Message(topic, messageBody.getBytes(IOUtil.DEFAULT_CHARSET));
    }

    Message(long traceID, final Topic topic, byte[] messageBody) {
        this.traceID = traceID;
        this.topic = Topic.newInstacne(topic, false);
        this.messageBody = messageBody;
    }

    Message(Topic topic, byte[] messageBody){
        this(0L, topic, messageBody);
    }

    public long getTraceID(){
        return this.traceID;
    }

    public String getTraceIDStr() {
        return String.valueOf(this.traceID);
    }

    public String getMessageBody(){
        return new String(this.messageBody, IOUtil.DEFAULT_CHARSET);
    }

    public byte[] getMessageBodyInByte(){
        return this.messageBody;
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

    public Message setDesiredTag(final DesiredTag desiredTag) {
        this.desiredTag = desiredTag.toString();
        return this;
    }

    public String getDesiredTag() {
        return this.desiredTag;
    }

    public byte[] getDesiredTagInByte() {
        if(null == this.desiredTag)
                return null;
        else {
           return this.desiredTag.getBytes(Charset.defaultCharset());
        }
    }

    /**
     * Set json header extension for message, for {@link com.youzan.nsq.client.core.command.PubExt} purpose.
     * json header ext ignored if current message is sent by commands other than {@link com.youzan.nsq.client.core.command.PubExt}
     * @param jsonExt {@link Object} for json parse
     */
    public void setJsonHeaderExt(final Object jsonExt) {
        this.jsonHeaderExt = jsonExt;
    }

    /**
     * get json header extension for message, for {@link com.youzan.nsq.client.core.command.PubExt} purpose.
     * json header ext ignored if current message is sent by commands other than {@link com.youzan.nsq.client.core.command.PubExt}
     * @return {@link Object}
     */
    public Object getJsonHeaderExt() {
        return this.jsonHeaderExt;
    }

    /**
     * Set topic sharding with {@link Long}
     * @param shardingIDLong sharding id long type
     * @return {@link Message}
     */
    public Message setTopicShardingIDLong(long shardingIDLong){
        return setTopicShardingIDObject(shardingIDLong);
    }

    /**
     * Set topic shardingID with {@link String}
     * @param shardingIDString shardingId String type
     * @return {@link Message}
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
