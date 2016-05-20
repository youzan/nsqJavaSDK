package com.youzan.nsq.client.core.command;

import java.util.ArrayList;
import java.util.List;

/**
 * Only one way of implementation
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface NSQCommand {

    public static final String ASCII = "US-ASCII";
    public static final String UTF = "UTF-8";
    public static final String DEFAULT_CHARSET_NAME = UTF;

    /**
     * The encoding between UTF-8 and US-ASCII is the same underlying
     * LINE_SEPARATOR.
     */
    static final byte LINE_SEPARATOR = '\n';
    static final List<byte[]> EMPTY_BODY = new ArrayList<>(0);

    // *************************************************************************
    // normal command
    // *************************************************************************

    /**
     * @return The binary data of sending to the NSQd (broker)
     */
    byte[] getBytes();

    // *************************************************************************
    // special command
    // *************************************************************************

    String getHeader();

    List<byte[]> getBody();

}
