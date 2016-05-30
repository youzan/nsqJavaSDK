/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.nio.ByteBuffer;
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

    private final byte[] data;

    /**
     * @param messageID
     * @param timeout
     *            seconds
     */
    public ReQueue(byte[] messageID, int timeout) {
        if (messageID == null || messageID.length <= 0) {
            throw new IllegalArgumentException("Your input messageID is empty!");
        }
        final byte[] cmd = "REQ ".getBytes(DEFAULT_CHARSET);
        final byte[] timeoutBytes = String.valueOf(timeout).getBytes(DEFAULT_CHARSET);
        final ByteBuffer bb = ByteBuffer.allocate(cmd.length + messageID.length + 1 + timeoutBytes.length + 1);
        // REQ <message_id> <timeout>\n
        bb.put(cmd).put(messageID).put(SPACE).put(timeoutBytes).put(LINE_SEPARATOR);
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
