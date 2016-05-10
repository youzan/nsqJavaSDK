package com.youzan.nsq.client.core.lookup;

import java.io.Closeable;
import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;

public interface NSQLookupService extends Closeable, java.io.Serializable {

    /**
     *
     * 
     * @param topic
     * @param writable
     * @return ordered NSQd-Server's addresses
     */
    SortedSet<Address> lookup(String topic, boolean writable);

    boolean save();

    boolean load();

}
