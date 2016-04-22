package com.youzan.nsq.client.commands;

import java.nio.ByteBuffer;

import com.youzan.nsq.client.enums.CommandType;
import com.youzan.nsq.client.remoting.connector.NSQConfig;

/**
 * @author caohaihong since 2015年10月29日 上午10:53:35
 */
public class Identify implements NSQCommand {
    private final String configString;
    private final byte[] configData;

    public Identify(NSQConfig config) {
        configString = config.toString();
        configData = config.toString().getBytes();
    }

    @Override
    public String getCommandString() {
        return String.format("%s\n%s", CommandType.IDENTIFY.getCode(), configString);
    }

    @Override
    public byte[] getCommandBytes() {
        String header = String.format("%s\n", CommandType.IDENTIFY.getCode());

        int size = configData.length;
        ByteBuffer bb = ByteBuffer.allocate(header.length() + 4 + size);
        bb.put(header.getBytes());
        bb.putInt(size);
        bb.put(configData);
        return bb.array();
    }
}
