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
public class Close implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Close.class);

    private final String cmd = "CLS\n";
    private final byte[] bytesCMD;

    private static class Instance {
        // final
        private static final Close instance = new Close();
    }

    public static Close getInstance() {
        return Instance.instance;
    }

    private Close() {
        byte[] tmp;
        try {
            tmp = cmd.getBytes(DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // Ugly Java
            logger.error("Exception", e);
            tmp = cmd.getBytes();
        }
        bytesCMD = tmp;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return null;
    }

    @Override
    public List<byte[]> getBody() {
        return null;
    }
}
