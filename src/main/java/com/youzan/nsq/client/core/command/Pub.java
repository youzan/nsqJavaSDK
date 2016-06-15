/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Pub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Pub.class);

    private final String topic;

    private final List<byte[]> body = new ArrayList<>(1);

    /**
     * @param topic
     *            the producer sets a topic name
     * @param data
     *            the producer publishes a raw message
     */
    public Pub(String topic, byte[] data) {
        this.topic = topic;
        this.body.add(data);
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return String.format("PUB %s\n", topic);
    }

    @Override
    public List<byte[]> getBody() {
        return this.body;
    }
}
