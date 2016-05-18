/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Nop implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Nop.class);
    private static final String cmd = "NOP\n";

    private static class Instance {
        // final
        private static final Nop instance = new Nop();
    }

    public static Nop getInstance() {
        return Instance.instance;
    }

    private Nop() {
    }

    @Override
    public String getString() {
        return cmd;
    }

    @Override
    public byte[] getBytes() {
        try {
            return cmd.getBytes(CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // Ugly Java
            logger.error("Exception", e);
        }
        return null;
    }
}
