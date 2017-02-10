package com.youzan.nsq.client.entity;

/**
 * Created by lin on 16/12/19.
 */
public interface IPartitionsSelector {

    /**
     * return array of {@link Partitions} dataNode in selector
     * @return {@link Partitions} array
     */
    Partitions[] choosePartitions();

    Partitions[] dumpAllPartitions();
}
