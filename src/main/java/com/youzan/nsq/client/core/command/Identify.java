/**
 * 
 */
package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Identify implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Identify.class);
    private static final byte[] IDENTITY_CMD = "IDENTIFY\n".getBytes(NSQCommand.DEFAULT_CHARSET);

    private final List<byte[]> body = new ArrayList<>(1);
    private byte[] bytes = null;
    public Identify(final NSQConfig config, boolean topicExt) {
        this.body.add(config.identify(topicExt).getBytes(DEFAULT_CHARSET));
    }

    @Override
    public byte[] getBytes() {
        if(bytes == null) {
            byte[] body = this.getBody().get(0);
            ByteBuffer buf = ByteBuffer.allocate(IDENTITY_CMD.length + 4 + body.length);
            buf.put(IDENTITY_CMD)
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
