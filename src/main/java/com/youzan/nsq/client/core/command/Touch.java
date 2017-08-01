package com.youzan.nsq.client.core.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by lin on 17/8/1.
 */
public class Touch implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Touch.class);

    private final byte[] data;

    public Touch(byte[] messageID) {
        if (messageID == null || messageID.length <= 0) {
            throw new IllegalArgumentException("Your input messageID is empty!");
        }
        final byte[] cmd = "TOUCH ".getBytes(UTF8);
        final ByteBuffer bb = ByteBuffer.allocate(cmd.length + messageID.length + 1);
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
