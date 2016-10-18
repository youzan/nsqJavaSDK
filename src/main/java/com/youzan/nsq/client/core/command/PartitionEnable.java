package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;

/**
 * partion context-aware interface for NSQCommands
 * Created by lin on 16/8/17.
 */
public interface PartitionEnable {
    //partition id 0, which is the smallest partition in order, according to current partition impl in NSQ
    public static int PARTITION_ID_SMALLEST = 0;
    //partition id no specify, which is the largest in order
    public static int PARTITION_ID_NO_SPECIFY = -1;

    /**
     * function to worry about how to convert partition id info
     * into bytes in Command, suggestion is that function need to
     * manager the format in the command, like space or line separator
     * @param topic
     * @return part of the command contains partition id
     */
    byte[] getPartitionIdByte(Topic topic);
}
