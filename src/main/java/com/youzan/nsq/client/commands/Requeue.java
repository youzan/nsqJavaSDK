package com.youzan.nsq.client.commands;

public class Requeue implements NSQCommand {

    private final byte[] msgId;
    private final int timeout;

    public Requeue(byte[] msgId, int timeout) {
        this.msgId = msgId;
        this.timeout = timeout;
    }

    @Override
    public String getCommandString() {
        final StringBuffer sb = new StringBuffer(50);
        sb.append("REQ ").append(new String(msgId)).append(" ");
        sb.append(timeout).append("\n");
        return sb.toString();
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
