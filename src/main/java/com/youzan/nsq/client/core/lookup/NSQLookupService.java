package com.youzan.nsq.client.core.lookup;

import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;

public interface NSQLookupService extends java.io.Serializable {

    /**
     *
     * lookup the writable NSQd (DataNode)
     * 
     * @param topic
     * @return ordered NSQd-Server's addresses
     */
    SortedSet<Address> lookup(String topic);

    /**
     * lookup the writable/non-writable NSQd (DataNode)
     * 
     * @param topic
     * @param writable
     * @return ordered NSQd-Server's addresses
     */
    SortedSet<Address> lookup(String topic, boolean writable);

    boolean save();

    boolean load();

}
