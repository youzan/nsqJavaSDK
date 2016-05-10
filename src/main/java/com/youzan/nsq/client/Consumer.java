package com.youzan.nsq.client;

import java.io.Closeable;
import java.util.List;

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
