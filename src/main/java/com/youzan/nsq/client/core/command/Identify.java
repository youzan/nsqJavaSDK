/**
 * 
 */
package com.youzan.nsq.client.core.command;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

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

    private final List<byte[]> body = new ArrayList<>(1);

    public Identify(NSQConfig config) throws UnsupportedEncodingException {
        byte[] tmp = config.identify().getBytes(DEFAULT_CHARSET_NAME);
        body.add(tmp);
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public String getHeader() {
        return "IDENTIFY\n";
    }

    @Override
    public List<byte[]> getBody() {
        return this.body;
    }

}
