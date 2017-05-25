/**
 * 
 */
package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Message;
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
    private final List<byte[]> body = new ArrayList<>(1);
    protected byte[] bytes = null;
    protected String desiredTag = null;
    protected int partitionOverride = -1;

    /**
     * @param msg
     *            message object
     */
    public Pub(Message msg) {
        this.topic = msg.getTopic();
        this.body.add(msg.getMessageBodyInByte());
        this.desiredTag = msg.getDesiredTag();
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

    /**
     * override default partition, by default, it should be used to override default partition(-1)
     * @param newPartition  new partition#
     */
    public void overrideDefaultPartition(int newPartition) {
        assert newPartition > -1;
        this.partitionOverride = newPartition;
    }

    protected String getPartitionAndTagStr() {
        String partitionStr;
        if(partitionOverride > -1)
            partitionStr = SPACE_STR + partitionOverride;
        else if(topic.hasPartition())
            partitionStr = SPACE_STR + topic.getPartitionId();
        else
            partitionStr = "";

        String tagFilterStr;
        if(!partitionStr.isEmpty()) {
            if(null != this.desiredTag &&  !this.desiredTag.isEmpty())
                tagFilterStr = SPACE_STR + this.desiredTag;
            else
                tagFilterStr = "";
        } else {
            tagFilterStr = "";
        }
        return partitionStr + tagFilterStr;
    }

    @Override
    public String getHeader() {
        return String.format("PUB %s%s\n", topic.getTopicText(), this.getPartitionAndTagStr());
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

    public String toString(){
        return this.getHeader();
    }
}
