package com.youzan.nsq.client.entity;

/**
 * Created by lin on 16/12/19.
 */
public interface IPartitionsSelector {

    /**
     * return one {@link Partitions} dataNode in selector
     * @return
     */
    Partitions choosePartitions();

    Partitions[] dumpAllPartitions();
}
