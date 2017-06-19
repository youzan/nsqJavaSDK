package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.exception.NSQPartitionNotAvailableException;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lin on 16/11/7.
 */
public class Partitions {
    private final String topic;
    //view of partition dataNode, length of dataNodes equals to size of Partitions.partitionNum;
    private Map<Integer, SoftReference<Address>> partitionId2Addr = null;
    private List<Address> dataNodes = null;
    //for compatible consideration, array for data node which has no partition
    private List<Address> unpartitionedDataNodes = null;
    //all data ndes view of partitions
    private List<Address> allDataNodes = null;
    //total partion number of topic
    private int partitionNum = 0;

    public Partitions(final String topic){
        this.topic = topic;
    }

    public Partitions updatePartitionDataNode(final Map<Integer, SoftReference<Address>> partitionId2Addr, final List<Address> dataNodes, final int partitionNum){
        if(null == dataNodes || dataNodes.size() == 0 || null == partitionId2Addr || partitionId2Addr.size() == 0)
            throw new RuntimeException("Length of pass in data nodes doe not match size of total partition number.");
        this.partitionId2Addr = partitionId2Addr;
        this.dataNodes = dataNodes;
        updatePartitionNum(partitionNum);
        return this;
    }

    /**
     * update partition number of current partition.
     * @param newPartitionNum new partition number.
     */
    public void updatePartitionNum(int newPartitionNum){
        if(newPartitionNum <= 0)
            return;
        this.partitionNum = newPartitionNum;
    }

    public Map<Integer, SoftReference<Address>> getPartitionId2Addr(){
        return this.partitionId2Addr;
    }

    public Partitions updateUnpartitionedDataNodea(final List<Address> dataNodes){
        if(null == dataNodes || dataNodes.size() == 0)
            throw new RuntimeException("Length of pass in data nodes does not match size of total partition number.");
        this.unpartitionedDataNodes = dataNodes;
        return this;
    }

    public Address getPartitionAddress(int partitionID) throws IndexOutOfBoundsException, NSQPartitionNotAvailableException {
        if(partitionID < 0 || partitionID >= this.partitionNum)
            throw new IndexOutOfBoundsException("PartitionID: " + partitionID + " out of boundary. Partition number: " + this.partitionNum);
        try {
            return this.partitionId2Addr.get(partitionID).get();
        } catch (NullPointerException npe) {
            throw new NSQPartitionNotAvailableException("Partition: " + partitionID + " not found for " + this.topic);
        }
    }

    public List<Address> getAllDataNodes(){
        if(null == this.allDataNodes) {
            int partitionsSize  = null == this.dataNodes ? 0 : this.dataNodes.size();
            int unpartitionsSize = null == this.unpartitionedDataNodes ? 0 : this.unpartitionedDataNodes.size();
            this.allDataNodes = new ArrayList<>(partitionsSize + unpartitionsSize);
            if(null != this.dataNodes)
                allDataNodes.addAll(this.dataNodes);
            if(null != this.unpartitionedDataNodes)
                allDataNodes.addAll(this.unpartitionedDataNodes);
        }
        return this.allDataNodes;
    }

    public List<Address> getUnpartitionedDataNodes(){
        return this.unpartitionedDataNodes;
    }

    public int getPartitionNum(){
        return this.partitionNum;
    }

    public boolean hasUnpartitionedDataNodes(){
        return null != this.unpartitionedDataNodes && this.unpartitionedDataNodes.size() > 0;
    }

    public boolean hasPartitionDataNodes(){
        return  null != this.dataNodes && this.dataNodes.size() > 0;
    }

    public boolean hasAnyDataNodes(){
        return hasPartitionDataNodes() || hasUnpartitionedDataNodes();
    }
}
