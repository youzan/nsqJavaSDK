/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Pub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Pub.class);

    private final String topic;

    private final List<byte[]> body = new ArrayList<>(1);

    /**
     * @param topic
     * @param data
     */
    public Pub(String topic, byte[] data) {
        super();
        this.topic = topic;
        body.add(data);
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return "PUB " + topic + LINE_SEPARATOR;
    }

    @Override
    public List<byte[]> getBody() {
        return this.body;
    }
}
