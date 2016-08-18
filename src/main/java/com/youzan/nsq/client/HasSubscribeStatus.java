package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.network.frame.MessageFrame;

/**
 * consumer implements this interface is intended to has SUB_ORDERED support,
 * Created by lin on 16/9/12.
 */
public interface HasSubscribeStatus {
    enum SubCmdType {
        SUB,
        SUB_ORDERED
    }

    SubCmdType getSubscribeStatus();


    /**
     * interface to create message for consumer according to pass in subscribe type(SUB, SUB_ORDERED...)
     * @param msgFrame
     * @param conn
     * @param subType
     * @return
     */
    NSQMessage createNSQMessage(final MessageFrame msgFrame, final NSQConnection conn, SubCmdType subType);

    /**
     * create subscribe command according to pass in subscribe type(SUB, SUB_ORDERED...)
     * @param subType
     * @param topic
     * @param channel
     * @return
     */
    Sub createSubCmd(final SubCmdType subType, Topic topic, String channel);
}
