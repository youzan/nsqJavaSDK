package com.youzan.nsq.client.commands;

public class Magic implements NSQCommand {

    @Override
    public String getCommandString() {
        return "  V2";
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
