/**
 * 
 */
package com.youzan.nsq.client.core.command;

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

    private final String topic;
    private final String channel;

    public Sub(String topic, String channel) {
        this.topic = topic;
        this.channel = channel;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return String.format("SUB %s %s\n", topic, channel);
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }

}
