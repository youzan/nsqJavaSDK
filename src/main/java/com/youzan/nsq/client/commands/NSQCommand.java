package com.youzan.nsq.client.commands;

public interface NSQCommand {
    String getCommandString();

    byte[] getCommandBytes();
}
