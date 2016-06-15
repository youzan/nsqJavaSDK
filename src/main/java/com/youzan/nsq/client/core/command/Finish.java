/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        final byte[] cmd = "FIN ".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer bb = ByteBuffer.allocate(cmd.length + messageID.length + 1);
        // FIN <message_id>\n
        bb.put(cmd).put(messageID).put(LINE_SEPARATOR);
        this.data = bb.array();
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
