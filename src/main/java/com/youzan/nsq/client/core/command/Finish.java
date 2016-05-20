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
    private final byte[] messageID;

    public Finish(byte[] messageID) {
        this.messageID = messageID;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }
}
