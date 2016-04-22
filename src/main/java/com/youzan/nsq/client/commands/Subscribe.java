package com.youzan.nsq.client.commands;


import com.youzan.nsq.client.enums.CommandType;

public class Subscribe implements NSQCommand {
    private String topic;
    private String channel;

    public Subscribe(String topic, String channel) {
        this.topic = topic;
        this.channel = channel;
    }

    @Override
    public String getCommandString() {
        return String.format("%s %s %s\n", CommandType.SUBSCRIBE.getCode(), topic, channel);
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
