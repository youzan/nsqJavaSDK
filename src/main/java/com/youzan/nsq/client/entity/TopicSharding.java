package com.youzan.nsq.client.entity;

/**
 * Created by lin on 16/11/7.
 */
public interface TopicSharding<T> {

    /**
     * return partition id in range of @param partitionIDs, given passin seed
     * @param passInSeed pass in seed
     * @param partitionNum total number of partitions.
     * @return partiton ID in range of @param partitionIDs
     */
    int toPartitionID(T passInSeed, final int partitionNum);

}
