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
public class Rdy implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Rdy.class);

    private final int count;

    public Rdy(int count) {
        this.count = count;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return String.format("RDY %d\n", count);
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }
}
