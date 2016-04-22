package com.youzan.nsq.client.commands;

import com.youzan.nsq.client.enums.CommandType;

public class Ready implements NSQCommand {
    private final int count;

    public Ready(int count) {
        this.count = count;
    }

    @Override
    public String getCommandString() {
        return String.format("%s %s\n", CommandType.READY.getCode(), count);
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
