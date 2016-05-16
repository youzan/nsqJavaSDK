package com.youzan.nsq.client.core.lookup;

import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;

public interface NSQLookupService extends java.io.Serializable {

    /**
     *
     * lookup the writable NSQd (DataNode)
     * 
     * @param topic
     * @return ordered NSQd-Server's addresses
     * @throws NSQLookupException
     */
    SortedSet<Address> lookup(String topic) throws NSQLookupException;

    /**
     * lookup the writable/non-writable NSQd (DataNode)
     * 
     * @param topic
     * @param writable
     * @return ordered NSQd-Server's addresses
     * @throws NSQLookupException
     */
    SortedSet<Address> lookup(String topic, boolean writable) throws NSQLookupException;

    boolean save();

    boolean load();

}
