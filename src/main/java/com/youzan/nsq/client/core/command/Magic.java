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
public class Magic implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Magic.class);

    private final String cmd = "  V2";
    private final byte[] asciiCMD;

    private static class Instance {
        // final
        private static final Magic instance = new Magic();
    }

    public static Magic getInstance() {
        return Instance.instance;
    }

    private Magic() {
        byte[] tmp;
        try {
            tmp = cmd.getBytes(ASCII);
        } catch (UnsupportedEncodingException e) {
            tmp = cmd.getBytes();
        }
        asciiCMD = tmp;
    }

    @Override
    public byte[] getBytes() {
        return asciiCMD;
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
