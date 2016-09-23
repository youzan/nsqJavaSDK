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
}
