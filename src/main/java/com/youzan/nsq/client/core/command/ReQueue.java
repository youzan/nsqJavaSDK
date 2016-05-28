/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ReQueue implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(ReQueue.class);

    private byte[] data;

    public ReQueue(byte[] messageID, int timeout) {
        if (messageID == null || messageID.length <= 0) {
            throw new IllegalArgumentException("Your input messageID is empty!");
        }
        byte[] tmp;
        try {
            final String d = String.format("REQ %s %d\n", new String(messageID, ASCII), timeout);
            tmp = d.getBytes(DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            logger.error("Exception", e);
            final String d = String.format("REQ %s %d\n", new String(messageID), timeout);
            tmp = d.getBytes();
        }
        this.data = tmp;
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
