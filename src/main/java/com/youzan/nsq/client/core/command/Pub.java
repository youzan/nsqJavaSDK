/**
 * 
 */
package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Pub implements NSQCommand, PartitionEnable {
    private static final Logger logger = LoggerFactory.getLogger(Pub.class);
    public final static int MSG_SIZE = 4;
    public final static int TRACE_ID_SIZE = 8;

    protected final Topic topic;

    protected final List<byte[]> body = new ArrayList<>(1);
    protected byte[] bytes = null;
    /**
     * @param topic
     *            the producer sets a topic name
     * @param data
     *            the producer publishes a raw message
     */
    public Pub(Topic topic, byte[] data) {
        this.topic = topic;
        this.body.add(data);
    }

    @Override
    /**
     * returns:
     * COMMAND HEADER
     * BODY
     *
     * in bytes
     */
    public byte[] getBytes() {
        if(null == bytes){
            byte[] header = this.getHeader().getBytes(NSQCommand.DEFAULT_CHARSET);
            byte[] body = this.getBody().get(0);
            ByteBuffer buf = ByteBuffer.allocate(header.length + 4 + body.length);

            buf.put(header)
                    .putInt(body.length)
                    .put(body);
            bytes = buf.array();
        }
        return bytes;
    }

    @Override
    public String getHeader() {
        return String.format("PUB %s%s\n", topic.getTopicText(), topic.hasPartition() ? SPACE_STR + topic.getPartitionId() : "");
    }

    @Override
    public List<byte[]> getBody() {
        return body;
    }

    protected String getTopicText() {
        return this.topic.getTopicText();
    }

    public Topic getTopic() {
        return this.topic;
    }

    /**
     * NOT implemented
     */
    @Override
    public byte[] getPartitionIdByte(Topic topic) {
        return null;
    }
}
