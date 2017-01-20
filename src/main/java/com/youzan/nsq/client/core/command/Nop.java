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
public class Nop implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Nop.class);

    private final byte[] data;

    private static class Instance {
        // final
        private static final Nop instance = new Nop();
    }

    public static Nop getInstance() {
        return Instance.instance;
    }

    private Nop() {
        this.data = "NOP\n".getBytes(DEFAULT_CHARSET);
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public String getHeader() {
        return null;
    }

    @Override
    public List<byte[]> getBody() {
        return EMPTY_BODY;
    }
}
