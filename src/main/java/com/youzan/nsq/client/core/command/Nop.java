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
public class Nop implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Nop.class);
    private final String cmd = "NOP\n";
    private final byte[] bytesCMD;

    private static class Instance {
        // final
        private static final Nop instance = new Nop();
    }

    public static Nop getInstance() {
        return Instance.instance;
    }

    private Nop() {
        byte[] tmp;
        try {
            tmp = cmd.getBytes(DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            tmp = cmd.getBytes();
        }
        bytesCMD = tmp;
    }

    @Override
    public byte[] getBytes() {
        return bytesCMD;
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
