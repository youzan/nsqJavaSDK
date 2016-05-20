/**
 * 
 */
package com.youzan.nsq.client.core.command;

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
    private final int size;
    private final byte[] data;

    /**
     * @param topic
     * @param size
     * @param data
     */
    public Pub(String topic, int size, byte[] data) {
        super();
        this.topic = topic;
        this.size = size;
        this.data = data;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }
}
