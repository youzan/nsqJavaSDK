/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.entity.NSQConfig;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Identify implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Identify.class);

    private static final String cmd = "IDENTIFY\n";
    private final String identifier;
    private byte[] data;

    public Identify(NSQConfig config) {
        this.identifier = config.identify();
        try {
            this.data = config.identify().getBytes(CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            // ignore
        }

    }

    @Override
    public String getString() {
        // JDK8
        return cmd + identifier;
    }

    @Override
    public byte[] getBytes() {
        final String header = cmd;
        final int size = data.length;
        ByteBuffer bb = ByteBuffer.allocate(header.length() + 4 + size);
        try {
            bb.put(header.getBytes(CHARSET_NAME));
        } catch (UnsupportedEncodingException e) {
            // Ugly Java
            logger.error("Exception", e);
            return null;
        }
        bb.putInt(size);
        bb.put(data);
        return bb.array();
    }

}
