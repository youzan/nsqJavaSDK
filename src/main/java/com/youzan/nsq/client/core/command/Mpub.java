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
public class Mpub implements NSQCommand{
    private static final Logger logger = LoggerFactory.getLogger(Mpub.class);

    private final Topic topic;
    private final List<byte[]> messages;

    public Mpub(Topic topic, List<byte[]> messages) {
        this.topic = topic;
        this.messages = messages;
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer buf = null;
        byte[] header = this.getHeader().getBytes(NSQCommand.DEFAULT_CHARSET);
        //get MPUB body, which is a list containing multi messages
        List<byte[]> bodyL = this.getBody();
        if (bodyL.size() > 1) {
            // write total body size and message size
            int bodySize = 4; // 4 for total messages int.
            for (byte[] data : bodyL) {
                bodySize += 4; // message size
                bodySize += data.length;
            }

            buf = ByteBuffer.allocate(bodySize);
            buf.put(header);
            buf.putInt(bodySize);
            buf.putInt(bodyL.size());
        }

        for (byte[] data : bodyL) {
            buf.putInt(data.length);
            buf.put(data);
        }

        return buf.array();
    }

    @Override
    public String getHeader() {
        return String.format("MPUB %s%s\n", topic.getTopicText(), topic.hasPartition() ? SPACE_STR + topic.getPartitionId() : "");
    }

    @Override
    public List<byte[]> getBody() {
        return messages;
    }
}
