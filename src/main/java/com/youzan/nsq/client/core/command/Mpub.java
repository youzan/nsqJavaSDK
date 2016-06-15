/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Mpub implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Mpub.class);

    private final String topic;
    private final List<byte[]> messages;

    public Mpub(String topic, List<byte[]> messages) {
        this.topic = topic;
        this.messages = messages;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return String.format("MPUB %s\n", topic);
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }
}
