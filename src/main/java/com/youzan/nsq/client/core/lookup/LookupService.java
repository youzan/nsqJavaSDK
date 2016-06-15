package com.youzan.nsq.client.core.lookup;

import java.util.Random;
import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;

/**
 * One lookup cluster
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public interface LookupService extends java.io.Serializable {

    static final Random _r = new Random(10000);

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

    /**
     * No exceptions.
     */
    void close();
}
