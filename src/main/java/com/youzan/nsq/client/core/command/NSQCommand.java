package com.youzan.nsq.client.core.command;

public interface NSQCommand {

    public static final String CHARSET_NAME = "US-ASCII";

    public static final String UTF8 = "UTF-8";

    String getString();

    byte[] getBytes();

    @Override
    String toString();
}
