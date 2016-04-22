package com.youzan.nsq.client.commands;

public class Finish implements NSQCommand {
    private final byte[] msgId;

    public Finish(byte[] msgId) {
        this.msgId = msgId;
    }

    @Override
    public String getCommandString() {
        return "FIN " + new String(msgId) + "\n";
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
