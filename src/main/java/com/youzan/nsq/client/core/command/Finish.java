/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Finish implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Finish.class);

    private final byte[] data;

    public Finish(byte[] messageID) {
        if (messageID == null || messageID.length <= 0) {
            throw new IllegalArgumentException("Your input messageID is empty!");
        }

        byte[] cmd;
        try {
            cmd = "FIN ".getBytes(DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // Ugly Java
            logger.error("Exception", e);
            cmd = "FIN ".getBytes();
        }
        final ByteBuffer buf = ByteBuffer.allocate(cmd.length + messageID.length + 1);

        buf.put(cmd);
        buf.put(messageID);
        buf.put(LINE_SEPARATOR);
        this.data = buf.array();
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
