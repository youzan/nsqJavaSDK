/**
 * 
 */
package com.youzan.nsq.client.core.command;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Finish implements NSQCommand {
    private final byte[] msgId;

    public Finish(byte[] msgId) {
        this.msgId = msgId;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }
}
