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
public class Mpub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Mpub.class);

    private final String topic;

    public Mpub(String topic) {
        this.topic = topic;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return "MPUB " + topic + LINE_SEPARATOR;
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }
}
