/**
 * 
 */
package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class ReQueue implements NSQCommand{
    private static final Logger logger = LoggerFactory.getLogger(ReQueue.class);

    private final byte[] data;

    /**
     * @param messageID
     *            the specified ID that passes to NSQd
     * @param timeoutInSecond
     *            the next consuming timeout
     */
    public ReQueue(byte[] messageID, int timeoutInSecond) {
        if (messageID == null || messageID.length <= 0) {
            throw new IllegalArgumentException("Your input messageID is empty!");
        }
        final byte[] cmd = "REQ ".getBytes(DEFAULT_CHARSET);
        final byte[] timeoutBytes = String.valueOf(timeoutInSecond * 1000).getBytes(DEFAULT_CHARSET);
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
