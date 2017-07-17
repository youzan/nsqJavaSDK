/**
 * 
 */
package com.youzan.nsq.client.core.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Rdy implements NSQCommand {
    private static final Logger logger = LoggerFactory.getLogger(Rdy.class);

    private final byte[] data;
    private final int count;
    public static final Rdy BACK_OFF = new Rdy(0);

    public Rdy(final int count) {
        this.count = count;
        final byte[] cmd = "RDY ".getBytes(UTF8);
        final byte[] countBytes = String.valueOf(count).getBytes(DEFAULT_CHARSET);
        final ByteBuffer bb = ByteBuffer.allocate(cmd.length + countBytes.length + 1);
        // RDY <count>\n
        bb.put(cmd).put(countBytes).put(LINE_SEPARATOR);
        this.data = bb.array();
    }

    public int getCount() {
        return this.count;
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

    @Override
    public String toString() {
        return "Rdy{" + "count=" + count + '}';
    }
}
