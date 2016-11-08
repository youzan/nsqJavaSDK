package com.youzan.nsq.client.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lin on 16/11/7.
 */
public class Partitions {
    private final Topic topic;
    //view of partition dataNode, length of dataNodes equals to size of Partitions.partitionNum;
    private Address[] dataNodes = new Address[0];
    //for compatible consideration, array for data node which has no partition
    private Address[] unpartitionedDataNodes = new Address[0];
    //all data ndes view of partitions
    private List<Address> allDataNodes = null;
    //total partion number of topic
    private int partitionNum = 0;

    public Partitions(final Topic topic){
        this.topic = topic;
    }

    public Partitions updatePartitionDataNode(final Address[] dataNodes, final int partitionNum){
        if(null == dataNodes || partitionNum != dataNodes.length)
            throw new RuntimeException("Length of pass in data nodes doe not match size of total partition number.");
        this.dataNodes = dataNodes;
        this.partitionNum = partitionNum;
        return this;
    }

    public Partitions updateUnpartitionedDataNodea(final Address[] dataNodes){
        if(null == dataNodes || dataNodes.length < 0)
            throw new RuntimeException("Length of pass in data nodes doe not match size of total partition number.");
        this.unpartitionedDataNodes = dataNodes;
        return this;
    }

    public Address getPartitionAddress(int partitionID) throws IndexOutOfBoundsException{
        if(partitionID < 0 || partitionID >= this.dataNodes.length)
            throw new IndexOutOfBoundsException("PartitionID: " + partitionID + " out of boundary. Partition number: " + this.partitionNum);
        return this.dataNodes[partitionID];
    }

    public List<Address> getAllDataNodes(){
        if(null == this.allDataNodes) {
            this.allDataNodes = new ArrayList<>(this.dataNodes.length + this.unpartitionedDataNodes.length);
            allDataNodes.addAll(Arrays.asList(this.dataNodes));
            allDataNodes.addAll(Arrays.asList(this.unpartitionedDataNodes));
        }
        return this.allDataNodes;
    }

    public Address[] getUnpartitionedDataNodes(){
        return this.unpartitionedDataNodes;
    }

    public int getPartitionNum(){
        return this.partitionNum;
    }

    public boolean hasUnpartitionedDataNodes(){
        return this.unpartitionedDataNodes.length > 0;
    }

    public boolean hasPartitionDataNodes(){
        return (this.dataNodes.length > 0 && this.partitionNum == this.dataNodes.length);
    }

    public boolean hasAnyDataNodes(){
        return hasPartitionDataNodes() || hasUnpartitionedDataNodes();
    }
}
