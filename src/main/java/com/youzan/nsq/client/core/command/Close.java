/**
 * 
 */
package com.youzan.nsq.client.core.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Close implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Close.class);

    private final byte[] data;

    private static class Instance {
        // final
        private static final Close instance = new Close();
    }

    public static Close getInstance() {
        return Instance.instance;
    }

    private Close() {
        this.data = "CLS\n".getBytes(DEFAULT_CHARSET);
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
