package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;

import java.nio.ByteBuffer;

/**
 * Created by lin on 16/9/11.
 */
public class SubOrdered extends Sub{

    public SubOrdered(Topic topic, String channel) {
        super(topic, channel);
    }

    @Override
    public byte[] getBytes() {
        if(null == this.data){
            final byte[] cmd = "SUB_ORDERED ".getBytes(DEFAULT_CHARSET);
            final byte[] topicBytes = topic.getTopicText().getBytes(DEFAULT_CHARSET);
            final byte[] channelBytes = channel.getBytes(DEFAULT_CHARSET);
            byte[] partitionBytes = null;
            partitionBytes = getPartitionIdByte(topic);
            //fixed buffer
            final ByteBuffer bb = ByteBuffer.allocate(cmd.length + topicBytes.length + 1 + channelBytes.length + 1 + partitionBytes.length);
            // SUB <topic_name> <channel_name> <partition id>\n
            bb.put(cmd).put(topicBytes).put(SPACE).put(channelBytes)
                    .put(partitionBytes)
                    .put(LINE_SEPARATOR);
            this.data = bb.array();
        }
        return data;
    }

}
