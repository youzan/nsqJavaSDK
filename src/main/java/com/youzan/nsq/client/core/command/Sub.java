/**
 * 
 */
package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Sub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Sub.class);

    protected byte[] data = null;
    protected Topic topic;
    protected String channel;

    public Sub(final Topic topic, final String channel) {
        this.topic = topic;
        this.channel = channel;
    }

    @Override
    public byte[] getBytes() {
        if(null == this.data){
            final byte[] cmd = "SUB ".getBytes(DEFAULT_CHARSET);
            final byte[] topicBytes = topic.getTopicText().getBytes(DEFAULT_CHARSET);
            final byte[] channelBytes = channel.getBytes(DEFAULT_CHARSET);
            final byte[] partitionBytes = getPartitionIdByte(topic);
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

    @Override
    public String getHeader() {
        return "";
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }

    public byte[] getPartitionIdByte(Topic topic) {
        return topic.hasPartition() ?
                (SPACE_STR + String.valueOf(topic.getPartitionId())).getBytes(DEFAULT_CHARSET) : new byte[0];
    }
}
