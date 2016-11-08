package com.youzan.nsq.client.core.lookup;

import com.youzan.nsq.client.entity.Partitions;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQLookupException;

import java.io.Closeable;
import java.util.Random;

/**
 * One lookup cluster
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public interface LookupService extends java.io.Serializable, Closeable {
    Random _r = new Random(10000);

    void start();

    /**
     *
     * lookup the writable NSQd (DataNode)
     * 
     * @param topic
     *            a topic object
     * @return ordered NSQd-Server's addresses
     * @throws NSQLookupException
     *             if an error occurs
     */
    Partitions lookup(Topic topic) throws NSQLookupException;

    /**
     * lookup the writable/non-writable NSQd (DataNode)
     * 
     * @param topic
     *            a topic object
     * @param writable
     *            set it boolean value
     * @return ordered NSQd-Server's addresses
     * @throws NSQLookupException
     *             if an error occurs
     */
    Partitions lookup(Topic topic, boolean writable) throws NSQLookupException;

    /**
     * Perform the action quietly. No exceptions.
     */
    @Override
    void close();
}
