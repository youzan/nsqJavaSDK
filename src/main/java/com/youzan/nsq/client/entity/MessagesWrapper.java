package com.youzan.nsq.client.entity;

import java.util.List;

/**
 * wrapper for multi message to be publish by {@link com.youzan.nsq.client.core.command.Mpub}
 * Created by lin on 17/11/1.
 */
public class MessagesWrapper extends Message {
    final private List<byte[]> messageBodiesInBytes;

    public MessagesWrapper(Topic topic, List<byte[]> messageBodies) {
        super(topic, null);
        this.messageBodiesInBytes = messageBodies;
    }

    @Override
    public List<byte[]> getMessageBodiesInByte() {
        return this.messageBodiesInBytes;
    }

    @Override
    public int getMessageCount() {
        return this.messageBodiesInBytes.size();
    }

    @Override
    public Message traced(){
        return this;
    }

    @Override
    public boolean isTraced(){
        return false;
    }

    public Message setTopicShardingIDObject(Object shardingIDObj){
        throw new IllegalArgumentException("setTopicShardingIDObject not support.");
    }

    @Override
    public Message setDesiredTag(final DesiredTag desiredTag) {
        throw new IllegalArgumentException("setDesiredTag not support.");
    }

    @Override
    public String getDesiredTag() {
        return null;
    }

    @Override
    public byte[] getDesiredTagInByte() {
        return null;
    }

    @Override
    public void setJsonHeaderExt(final Object jsonExt) {
        throw new IllegalArgumentException("setJsonHeaderExt not support");
    }

    @Override
    public Object getJsonHeaderExt() {
        return null;
    }

   @Override
    public Message setTopicShardingIDLong(long shardingIDLong){
        return setTopicShardingIDObject(shardingIDLong);
    }

    @Override
    public Message setTopicShardingIDString(String shardingIDString){
        return setTopicShardingIDObject(shardingIDString);
    }

    @Override
    public Object getTopicShardingId(){
        return NO_SHARDING;
    }
}
