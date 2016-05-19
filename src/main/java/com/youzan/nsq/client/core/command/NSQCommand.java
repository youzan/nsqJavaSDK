package com.youzan.nsq.client.core.command;

public interface NSQCommand {

    public static final String UTF8 = "UTF-8";
    public static final String ASCII = "US-ASCII";
    public static final String DEFAULT_CHARSET_NAME = UTF8;

    String getString();

    /**
     * Using binary data to send to NSQ-Server
     * 
     * @return
     */
    byte[] getBytes();

}
