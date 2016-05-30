/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Sub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Sub.class);

    private final byte[] data;

    public Sub(final String topic, final String channel) {
        final byte[] cmd = "SUB ".getBytes(DEFAULT_CHARSET);
        final byte[] topicBytes = topic.getBytes(DEFAULT_CHARSET);
        final byte[] channelBytes = topic.getBytes(DEFAULT_CHARSET);
        final ByteBuffer bb = ByteBuffer.allocate(cmd.length + topicBytes.length + 1 + channelBytes.length + 1);
        // SUB <topic_name> <channel_name>\n
        bb.put(cmd).put(topicBytes).put(SPACE).put(channelBytes).put(LINE_SEPARATOR);
        this.data = bb.array();
    }

    @Override
    public byte[] getBytes() {
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

}
