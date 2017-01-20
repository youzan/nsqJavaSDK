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
public class Magic implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Magic.class);

    private final byte[] data;

    private static class Instance {
        // final
        private static final Magic instance = new Magic();
    }

    public static Magic getInstance() {
        return Instance.instance;
    }

    private Magic() {
        this.data = "  V2".getBytes(ASCII);
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
