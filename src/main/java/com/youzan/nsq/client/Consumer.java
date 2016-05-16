package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

/**
 * We use one connection to Because of the synchronized protocol and avoid the
 * complex handler when errors occur
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface Consumer extends Closeable {

    Consumer start();

    /**
     * notify the NSQ-Server that turrning off pushing some messages 
     */
    void backoff();

    /**
     * 
     * @param addresses
     */
    void addLookupCluster(List<String> addresses);

}
