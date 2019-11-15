package com.youzan.nsq.client.core.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Auth implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);
    private static final byte[] AUTH_CMD = "AUTH\n".getBytes(NSQCommand.DEFAULT_CHARSET);
    public static final String IDENTITY = "identity";
    public static final String IDENTITY_URL = "identity_url";
    public static final String PERMISSION_COUNT = "permission_count";
    private byte[] bytes = null;
    private final List<byte[]> body = new ArrayList<>(1);

    public Auth(String secret) {
        this.body.add(secret.getBytes(DEFAULT_CHARSET));
    }

    @Override
    public byte[] getBytes() {
        if (bytes == null) {
            byte[] body = this.getBody().get(0);
            ByteBuffer buf = ByteBuffer.allocate(AUTH_CMD.length + 4 + body.length);
            buf.put(AUTH_CMD)
                    .putInt(body.length)
                    .put(body);
            bytes = buf.array();
        }

        return bytes;
    }

    @Override
    public String getHeader() {
        return "";
    }

    @Override
    public List<byte[]> getBody() {
        return this.body;
    }
}
