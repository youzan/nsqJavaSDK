package com.youzan.nsq.client.core.command;

import com.youzan.nsq.client.entity.Topic;

/**
 * partion context-aware interface for NSQCommands
 * Created by lin on 16/8/17.
 */
public interface PartitionEnable {

    /**
     * function to worry about how to convert partition id info
     * into bytes in Command, suggestion is that function need to
     * manager the format in the command, like space or line separator
     * @param topic topic
     * @return part of the command contains partition id
     */
    byte[] getPartitionIdByte(Topic topic);
}
