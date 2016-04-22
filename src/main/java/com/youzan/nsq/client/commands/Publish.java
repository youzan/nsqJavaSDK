package com.youzan.nsq.client.commands;

import java.nio.ByteBuffer;

import com.youzan.nsq.client.enums.CommandType;

public class Publish implements NSQCommand {
    private final String topic;
    private final byte[] data;

    public Publish(String topic, byte[] data) {
        this.topic = topic;
        this.data = data;
    }

    @Override
    public String getCommandString() {
        return String.format("%s %s\n%s%s", CommandType.PUBLISH.getCode(), topic, data.length, data);
    }

    @Override
    public byte[] getCommandBytes() {
        String header = String.format("%s %s\n", CommandType.PUBLISH.getCode(), topic);

        int size = data.length;
        ByteBuffer bb = ByteBuffer.allocate(header.length() + 4 + size);
        bb.put(header.getBytes());
        bb.putInt(size);
        bb.put(data);
        return bb.array();
    }

}
